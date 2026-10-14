//! PostgreSQL text-value decoding at the capture boundary.
//!
//! Returned bytes use the closed M0 scalar encoding. Unsupported values fail before a journal
//! commit; callers must never persist the source text as a substitute for canonical bytes.

use crate::m1_ordering::CanonicalValue;
use chrono::{DateTime, NaiveDate, NaiveDateTime};

const MAX_SCALAR_BYTES: usize = 1_048_576;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueError {
    UnsupportedType,
    InvalidValue,
    Limit,
}

pub fn canonical_scalar(
    type_oid: u32,
    type_modifier: i32,
    wire: &[u8],
) -> Result<CanonicalValue, ValueError> {
    if wire.len() > MAX_SCALAR_BYTES {
        return Err(ValueError::Limit);
    }
    if let Some(element_oid) = array_element_oid(type_oid) {
        return Ok(CanonicalValue {
            type_oid,
            type_modifier,
            bytes: canonical_array(element_oid, wire)?,
        });
    }
    let text = std::str::from_utf8(wire).map_err(|_| ValueError::InvalidValue)?;
    let bytes = match type_oid {
        16 => match text {
            "t" => vec![1],
            "f" => vec![0],
            _ => return Err(ValueError::InvalidValue),
        },
        21 => text
            .parse::<i16>()
            .map(i16::to_be_bytes)
            .map(Vec::from)
            .map_err(|_| ValueError::InvalidValue)?,
        23 => text
            .parse::<i32>()
            .map(i32::to_be_bytes)
            .map(Vec::from)
            .map_err(|_| ValueError::InvalidValue)?,
        20 => text
            .parse::<i64>()
            .map(i64::to_be_bytes)
            .map(Vec::from)
            .map_err(|_| ValueError::InvalidValue)?,
        26 => text
            .parse::<u32>()
            .map(u32::to_be_bytes)
            .map(Vec::from)
            .map_err(|_| ValueError::InvalidValue)?,
        700 => {
            let value = text.parse::<f32>().map_err(|_| ValueError::InvalidValue)?;
            if value.is_nan() {
                0x7fc0_0000_u32.to_be_bytes().to_vec()
            } else {
                value.to_bits().to_be_bytes().to_vec()
            }
        }
        701 => {
            let value = text.parse::<f64>().map_err(|_| ValueError::InvalidValue)?;
            if value.is_nan() {
                0x7ff8_0000_0000_0000_u64.to_be_bytes().to_vec()
            } else {
                value.to_bits().to_be_bytes().to_vec()
            }
        }
        1700 => canonical_numeric(text)?.into_bytes(),
        1082 => pg_date(text)?.to_be_bytes().to_vec(),
        1114 => pg_timestamp(text)?.to_be_bytes().to_vec(),
        1184 => pg_timestamptz(text)?.to_be_bytes().to_vec(),
        2950 => uuid_bytes(text)?,
        25 | 1043 | 1042 | 19 => wire.to_vec(),
        17 => bytea_bytes(text)?,
        _ => return Err(ValueError::UnsupportedType),
    };
    if bytes.len() > MAX_SCALAR_BYTES {
        return Err(ValueError::Limit);
    }
    Ok(CanonicalValue {
        type_oid,
        type_modifier,
        bytes,
    })
}

fn array_element_oid(array_oid: u32) -> Option<u32> {
    Some(match array_oid {
        1000 => 16,
        1005 => 21,
        1007 => 23,
        1016 => 20,
        1028 => 26,
        1021 => 700,
        1022 => 701,
        1231 => 1700,
        1182 => 1082,
        1115 => 1114,
        1185 => 1184,
        2951 => 2950,
        1009 => 25,
        1015 => 1043,
        1014 => 1042,
        1003 => 19,
        1001 => 17,
        _ => return None,
    })
}

fn canonical_array(element_oid: u32, wire: &[u8]) -> Result<Vec<u8>, ValueError> {
    let (expected_count, body) = if wire.first() == Some(&b'[') {
        let end = wire
            .iter()
            .position(|&byte| byte == b']')
            .ok_or(ValueError::InvalidValue)?;
        if wire.get(end + 1) != Some(&b'=') {
            return Err(ValueError::InvalidValue);
        }
        let bounds = std::str::from_utf8(&wire[1..end]).map_err(|_| ValueError::InvalidValue)?;
        let (lower, upper) = bounds.split_once(':').ok_or(ValueError::InvalidValue)?;
        if lower != "1" {
            return Err(ValueError::InvalidValue);
        }
        let upper = upper.parse::<i64>().map_err(|_| ValueError::InvalidValue)?;
        if upper < 0 || upper > MAX_SCALAR_BYTES as i64 {
            return Err(ValueError::Limit);
        }
        (Some(upper as usize), &wire[end + 2..])
    } else {
        (None, wire)
    };
    if body.len() < 2 || body.first() != Some(&b'{') || body.last() != Some(&b'}') {
        return Err(ValueError::InvalidValue);
    }
    let body = &body[1..body.len() - 1];
    let mut values = Vec::new();
    let mut at = 0;
    while at < body.len() {
        let quoted = body[at] == b'"';
        if quoted {
            at += 1;
        }
        let mut value = Vec::new();
        let mut closed = !quoted;
        while at < body.len() {
            let byte = body[at];
            at += 1;
            if byte == b'\\' {
                let next = *body.get(at).ok_or(ValueError::InvalidValue)?;
                value.push(next);
                at += 1;
            } else if quoted && byte == b'"' {
                closed = true;
                break;
            } else if !quoted && byte == b',' {
                at -= 1;
                break;
            } else if !quoted && matches!(byte, b'{' | b'}' | b'"') {
                return Err(ValueError::InvalidValue);
            } else {
                value.push(byte);
            }
        }
        if !closed || (!quoted && value.is_empty()) {
            return Err(ValueError::InvalidValue);
        }
        values.push(if !quoted && value == b"NULL" {
            None
        } else {
            Some(value)
        });
        if at == body.len() {
            break;
        }
        if body[at] != b',' || at + 1 == body.len() {
            return Err(ValueError::InvalidValue);
        }
        at += 1;
    }
    if expected_count.is_some_and(|expected| expected != values.len()) {
        return Err(ValueError::InvalidValue);
    }
    let count = u32::try_from(values.len()).map_err(|_| ValueError::Limit)?;
    let mut encoded = Vec::new();
    encoded.extend(count.to_be_bytes());
    for value in values {
        match value {
            None => encoded.push(1),
            Some(value) => {
                let element = canonical_scalar(element_oid, -1, &value)?;
                encoded.push(3);
                encoded.extend(element_oid.to_be_bytes());
                encoded.extend((-1_i32).to_be_bytes());
                encoded.extend((element.bytes.len() as u64).to_be_bytes());
                encoded.extend(element.bytes);
            }
        }
        if encoded.len() > MAX_SCALAR_BYTES {
            return Err(ValueError::Limit);
        }
    }
    Ok(encoded)
}

fn canonical_numeric(text: &str) -> Result<String, ValueError> {
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent
                .parse::<i64>()
                .map_err(|_| ValueError::InvalidValue)?,
        ),
        None => (unsigned, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (integer.is_empty() && fraction.is_empty())
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(ValueError::InvalidValue);
    }
    let digits = format!("{integer}{fraction}");
    if digits.bytes().all(|byte| byte == b'0') {
        return Ok("0".into());
    }
    let decimal_at = i64::try_from(integer.len())
        .map_err(|_| ValueError::Limit)?
        .checked_add(exponent)
        .ok_or(ValueError::Limit)?;
    if decimal_at.unsigned_abs() > MAX_SCALAR_BYTES as u64 || digits.len() > MAX_SCALAR_BYTES {
        return Err(ValueError::Limit);
    }
    let mut result = if decimal_at <= 0 {
        format!("0.{}{digits}", "0".repeat((-decimal_at) as usize))
    } else if decimal_at as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(decimal_at as usize - digits.len()))
    } else {
        format!(
            "{}.{}",
            &digits[..decimal_at as usize],
            &digits[decimal_at as usize..]
        )
    };
    if let Some((left, right)) = result.split_once('.') {
        let left = left.trim_start_matches('0');
        let right = right.trim_end_matches('0');
        result = if right.is_empty() {
            if left.is_empty() { "0" } else { left }.to_owned()
        } else {
            format!("{}.{}", if left.is_empty() { "0" } else { left }, right)
        };
    } else {
        result = result.trim_start_matches('0').to_owned();
        if result.is_empty() {
            result.push('0');
        }
    }
    if result.len() > MAX_SCALAR_BYTES {
        return Err(ValueError::Limit);
    }
    if negative && result != "0" {
        result.insert(0, '-');
    }
    Ok(result)
}

fn pg_date(text: &str) -> Result<i32, ValueError> {
    match text {
        "-infinity" => return Ok(i32::MIN),
        "infinity" => return Ok(i32::MAX),
        _ => {}
    }
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| ValueError::InvalidValue)?;
    let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid PostgreSQL epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(|_| ValueError::Limit)
}

fn pg_timestamp(text: &str) -> Result<i64, ValueError> {
    match text {
        "-infinity" => return Ok(i64::MIN),
        "infinity" => return Ok(i64::MAX),
        _ => {}
    }
    let value = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|_| ValueError::InvalidValue)?;
    let epoch = NaiveDate::from_ymd_opt(2000, 1, 1)
        .expect("valid PostgreSQL epoch")
        .and_hms_opt(0, 0, 0)
        .expect("valid epoch time");
    value
        .signed_duration_since(epoch)
        .num_microseconds()
        .ok_or(ValueError::Limit)
}

fn pg_timestamptz(text: &str) -> Result<i64, ValueError> {
    match text {
        "-infinity" => return Ok(i64::MIN),
        "infinity" => return Ok(i64::MAX),
        _ => {}
    }
    let rfc = text.replacen(' ', "T", 1);
    let value = DateTime::parse_from_rfc3339(&rfc)
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .map_err(|_| ValueError::InvalidValue)?;
    let pg_epoch_micros = 946_684_800_000_000_i64;
    value
        .timestamp_micros()
        .checked_sub(pg_epoch_micros)
        .ok_or(ValueError::Limit)
}

fn uuid_bytes(text: &str) -> Result<Vec<u8>, ValueError> {
    let bytes = text.as_bytes();
    if bytes.len() != 36 || [8, 13, 18, 23].iter().any(|&at| bytes[at] != b'-') {
        return Err(ValueError::InvalidValue);
    }
    let mut hex = Vec::with_capacity(32);
    for (at, &byte) in bytes.iter().enumerate() {
        if ![8, 13, 18, 23].contains(&at) {
            hex.push(byte);
        }
    }
    decode_hex(&hex)
}

fn bytea_bytes(text: &str) -> Result<Vec<u8>, ValueError> {
    let hex = text.strip_prefix("\\x").ok_or(ValueError::InvalidValue)?;
    decode_hex(hex.as_bytes())
}

fn decode_hex(hex: &[u8]) -> Result<Vec<u8>, ValueError> {
    if !hex.len().is_multiple_of(2) {
        return Err(ValueError::InvalidValue);
    }
    hex.chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char)
                .to_digit(16)
                .ok_or(ValueError::InvalidValue)?;
            let lo = (pair[1] as char)
                .to_digit(16)
                .ok_or(ValueError::InvalidValue)?;
            Ok(((hi << 4) | lo) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m0_scalar_vectors_match_pg_text_decoding() {
        let cases = [
            (16, "t", "01"),
            (21, "-2", "fffe"),
            (23, "65537", "00010001"),
            (20, "-1", "ffffffffffffffff"),
            (26, "42", "0000002a"),
            (700, "NaN", "7fc00000"),
            (701, "-0.0", "8000000000000000"),
            (1700, "-123.4500", "2d3132332e3435"),
            (1082, "2000-01-02", "00000001"),
            (1114, "2000-01-01 00:00:00.000001", "0000000000000001"),
            (1184, "1999-12-31 23:59:59.999999+00:00", "ffffffffffffffff"),
            (
                2950,
                "00112233-4455-6677-8899-aabbccddeeff",
                "00112233445566778899aabbccddeeff",
            ),
            (25, "abc", "616263"),
            (17, "\\x00ff", "00ff"),
            (
                1007,
                "{1,NULL}",
                "000000020300000017ffffffff00000000000000040000000101",
            ),
        ];
        for (oid, wire, expected) in cases {
            let value = canonical_scalar(oid, -1, wire.as_bytes()).unwrap();
            let actual: String = value
                .bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(actual, expected, "OID {oid}");
        }
    }

    #[test]
    fn malformed_or_unadmitted_values_fail_closed() {
        for (oid, wire) in [
            (3802, "{}"),
            (16, "true"),
            (21, "32768"),
            (1700, "NaN"),
            (1082, "not a date"),
            (2950, "short"),
            (17, "\\x0"),
            (1007, "{{1}}"),
            (1007, "[0:0]={1}"),
            (1007, "{1,}"),
        ] {
            assert!(canonical_scalar(oid, -1, wire.as_bytes()).is_err());
        }
        assert_eq!(canonical_numeric("-0.000e10").unwrap(), "0");
        assert_eq!(canonical_numeric("1.2300e2").unwrap(), "123");
        assert_eq!(canonical_numeric(".00120").unwrap(), "0.0012");
        assert_eq!(canonical_numeric("0e-9223372036854775808").unwrap(), "0");
        assert_eq!(
            canonical_numeric("1e-9223372036854775808"),
            Err(ValueError::Limit)
        );
    }

    #[test]
    fn array_text_preserves_quoted_delimiters_and_null_distinction() {
        let value = canonical_scalar(1009, -1, br#"{"a,b","NULL",NULL,"a\"b"}"#).unwrap();
        let mut expected = Vec::from(4_u32.to_be_bytes());
        for element in [
            Some(&b"a,b"[..]),
            Some(&b"NULL"[..]),
            None,
            Some(&b"a\"b"[..]),
        ] {
            match element {
                Some(bytes) => {
                    expected.push(3);
                    expected.extend(25_u32.to_be_bytes());
                    expected.extend((-1_i32).to_be_bytes());
                    expected.extend((bytes.len() as u64).to_be_bytes());
                    expected.extend(bytes);
                }
                None => expected.push(1),
            }
        }
        assert_eq!(value.bytes, expected);
        assert_eq!(
            canonical_scalar(1007, -1, b"[1:2]={1,2}").unwrap().bytes,
            canonical_scalar(1007, -1, b"{1,2}").unwrap().bytes
        );
    }
}
