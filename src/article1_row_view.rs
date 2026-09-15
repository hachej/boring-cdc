//! `article1_row_view` is a **TEACHING VIEW** over Article 1's live decoded events.
//!
//! It consumes the same in-process [`PgoutputEvent`](crate::m1_decoder::PgoutputEvent) /
//! [`RowChange`](crate::m1_decoder::RowChange) values that are rendered as raw reader output.
//! It does not parse SQL, transcripts, or rendered JSONL. It is **NOT ClickHouse, NOT durable,
//! NOT exactly-once, NOT checkpointed, NOT a materializer, NOT production state, and NOT M4**.
//! ClickHouse and destination guarantees are deferred to Article 4/M4.

use crate::m1_decoder::{OldTupleKind, RelationContract, RowChange, RowKind, TupleValue};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum KeyPart {
    Null,
    Text(Vec<u8>),
}

type RowKey = (u32, Vec<KeyPart>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowViewFailure {
    pub code: &'static str,
}

impl fmt::Display for RowViewFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for RowViewFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowViewChange {
    Current {
        key: Vec<TupleValue>,
        row: Vec<TupleValue>,
    },
    Removed {
        key: Vec<TupleValue>,
        row: Vec<TupleValue>,
    },
}

/// Process-local current rows used only to explain how a consumer can interpret row changes.
pub struct Article1RowView {
    key_columns: BTreeMap<u32, Vec<usize>>,
    rows: BTreeMap<RowKey, Vec<TupleValue>>,
}

impl Article1RowView {
    pub fn new(contracts: &BTreeMap<u32, RelationContract>) -> Self {
        Self {
            key_columns: contracts
                .iter()
                .map(|(relation_id, contract)| (*relation_id, contract.key_columns.clone()))
                .collect(),
            rows: BTreeMap::new(),
        }
    }

    /// Apply one live decoded row change, rejecting any state that cannot be interpreted honestly.
    pub fn apply(&mut self, change: &RowChange) -> Result<RowViewChange, RowViewFailure> {
        let key_columns = self
            .key_columns
            .get(&change.relation_id)
            .ok_or(RowViewFailure {
                code: "ARTICLE1_ROW_VIEW_RELATION_UNKNOWN",
            })?;
        if key_columns.is_empty() {
            return Err(RowViewFailure {
                code: "ARTICLE1_ROW_VIEW_KEY_UNAVAILABLE",
            });
        }

        match change.kind {
            RowKind::Insert => {
                let row = complete_new(change)?;
                let (storage_key, key) = extract_key(change.relation_id, key_columns, &row)?;
                if self.rows.contains_key(&storage_key) {
                    return Err(RowViewFailure {
                        code: "ARTICLE1_ROW_VIEW_INSERT_ALREADY_EXISTS",
                    });
                }
                self.rows.insert(storage_key, row.clone());
                Ok(RowViewChange::Current { key, row })
            }
            RowKind::Update => {
                let incoming = change.new.as_ref().ok_or(RowViewFailure {
                    code: "ARTICLE1_ROW_VIEW_UPDATE_NEW_UNAVAILABLE",
                })?;
                let lookup_values = match (&change.old_kind, &change.old) {
                    (None, None) => incoming,
                    (Some(OldTupleKind::Key | OldTupleKind::Full), Some(old)) => old,
                    _ => {
                        return Err(RowViewFailure {
                            code: "ARTICLE1_ROW_VIEW_UPDATE_OLD_INCONSISTENT",
                        });
                    }
                };
                let (lookup_key, _) = extract_key(change.relation_id, key_columns, lookup_values)?;
                let previous = self.rows.remove(&lookup_key).ok_or(RowViewFailure {
                    code: "ARTICLE1_ROW_VIEW_UPDATE_CURRENT_UNAVAILABLE",
                })?;
                let row = merge_update(incoming, &previous)?;
                let (new_key, key) = extract_key(change.relation_id, key_columns, &row)?;
                if new_key != lookup_key && self.rows.contains_key(&new_key) {
                    self.rows.insert(lookup_key, previous);
                    return Err(RowViewFailure {
                        code: "ARTICLE1_ROW_VIEW_UPDATE_KEY_CONFLICT",
                    });
                }
                self.rows.insert(new_key, row.clone());
                Ok(RowViewChange::Current { key, row })
            }
            RowKind::Delete => {
                let old = change.old.as_ref().ok_or(RowViewFailure {
                    code: "ARTICLE1_ROW_VIEW_DELETE_OLD_UNAVAILABLE",
                })?;
                if change.old_kind.is_none() {
                    return Err(RowViewFailure {
                        code: "ARTICLE1_ROW_VIEW_DELETE_OLD_INCONSISTENT",
                    });
                }
                let (storage_key, key) = extract_key(change.relation_id, key_columns, old)?;
                let row = self.rows.remove(&storage_key).ok_or(RowViewFailure {
                    code: "ARTICLE1_ROW_VIEW_DELETE_CURRENT_UNAVAILABLE",
                })?;
                Ok(RowViewChange::Removed { key, row })
            }
        }
    }
}

fn complete_new(change: &RowChange) -> Result<Vec<TupleValue>, RowViewFailure> {
    let row = change.new.clone().ok_or(RowViewFailure {
        code: "ARTICLE1_ROW_VIEW_INSERT_NEW_UNAVAILABLE",
    })?;
    if row
        .iter()
        .any(|value| matches!(value, TupleValue::UnchangedToast))
    {
        return Err(RowViewFailure {
            code: "ARTICLE1_ROW_VIEW_INSERT_TOAST_UNAVAILABLE",
        });
    }
    Ok(row)
}

fn merge_update(
    incoming: &[TupleValue],
    previous: &[TupleValue],
) -> Result<Vec<TupleValue>, RowViewFailure> {
    if incoming.len() != previous.len() {
        return Err(RowViewFailure {
            code: "ARTICLE1_ROW_VIEW_UPDATE_SHAPE_MISMATCH",
        });
    }
    Ok(incoming
        .iter()
        .zip(previous)
        .map(|(next, old)| match next {
            TupleValue::UnchangedToast => old.clone(),
            value => value.clone(),
        })
        .collect())
}

fn extract_key(
    relation_id: u32,
    key_columns: &[usize],
    row: &[TupleValue],
) -> Result<(RowKey, Vec<TupleValue>), RowViewFailure> {
    let mut storage = Vec::with_capacity(key_columns.len());
    let mut visible = Vec::with_capacity(key_columns.len());
    for index in key_columns {
        let value = row.get(*index).ok_or(RowViewFailure {
            code: "ARTICLE1_ROW_VIEW_KEY_COLUMN_UNAVAILABLE",
        })?;
        storage.push(match value {
            TupleValue::Null => KeyPart::Null,
            TupleValue::Text(bytes) => KeyPart::Text(bytes.clone()),
            TupleValue::UnchangedToast => {
                return Err(RowViewFailure {
                    code: "ARTICLE1_ROW_VIEW_KEY_TOAST_UNAVAILABLE",
                });
            }
        });
        visible.push(value.clone());
    }
    Ok(((relation_id, storage), visible))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_decoder::{Column, Relation};

    fn text(value: &str) -> TupleValue {
        TupleValue::Text(value.as_bytes().to_vec())
    }

    fn view() -> Article1RowView {
        let relation = Relation {
            id: 7,
            namespace: "public".into(),
            name: "customers".into(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    key: true,
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                },
                Column {
                    key: false,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        Article1RowView::new(&BTreeMap::from([(
            7,
            RelationContract {
                relation,
                key_columns: vec![0],
                control: None,
            },
        )]))
    }

    fn change(
        kind: RowKind,
        old_kind: Option<OldTupleKind>,
        old: Option<Vec<TupleValue>>,
        new: Option<Vec<TupleValue>>,
    ) -> RowChange {
        RowChange {
            xid: 1,
            ordinal: 0,
            relation_id: 7,
            kind,
            old_kind,
            old,
            new,
        }
    }

    #[test]
    fn insert_update_absent_and_delete_key_produce_current_state() {
        let mut view = view();
        assert_eq!(
            view.apply(&change(
                RowKind::Insert,
                None,
                None,
                Some(vec![text("1"), text("before")])
            ))
            .unwrap(),
            RowViewChange::Current {
                key: vec![text("1")],
                row: vec![text("1"), text("before")]
            }
        );
        assert_eq!(
            view.apply(&change(
                RowKind::Update,
                None,
                None,
                Some(vec![text("1"), text("after")])
            ))
            .unwrap(),
            RowViewChange::Current {
                key: vec![text("1")],
                row: vec![text("1"), text("after")]
            }
        );
        assert_eq!(
            view.apply(&change(
                RowKind::Delete,
                Some(OldTupleKind::Key),
                Some(vec![text("1"), TupleValue::Null]),
                None
            ))
            .unwrap(),
            RowViewChange::Removed {
                key: vec![text("1")],
                row: vec![text("1"), text("after")]
            }
        );
    }

    #[test]
    fn full_identity_and_unchanged_toast_are_explicit_and_correct() {
        let mut view = view();
        view.apply(&change(
            RowKind::Insert,
            None,
            None,
            Some(vec![text("1"), text("before")]),
        ))
        .unwrap();
        let updated = view
            .apply(&change(
                RowKind::Update,
                Some(OldTupleKind::Full),
                Some(vec![text("1"), text("before")]),
                Some(vec![text("1"), TupleValue::UnchangedToast]),
            ))
            .unwrap();
        assert_eq!(
            updated,
            RowViewChange::Current {
                key: vec![text("1")],
                row: vec![text("1"), text("before")]
            }
        );
    }

    #[test]
    fn insufficient_update_and_delete_state_fail_instead_of_guessing() {
        let mut view = view();
        assert_eq!(
            view.apply(&change(
                RowKind::Update,
                None,
                None,
                Some(vec![text("1"), text("after")])
            ))
            .unwrap_err()
            .code,
            "ARTICLE1_ROW_VIEW_UPDATE_CURRENT_UNAVAILABLE"
        );
        assert_eq!(
            view.apply(&change(RowKind::Delete, None, None, None))
                .unwrap_err()
                .code,
            "ARTICLE1_ROW_VIEW_DELETE_OLD_UNAVAILABLE"
        );
    }
}
