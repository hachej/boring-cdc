use boring_cdc::m4_clickhouse_schema::{
    Principal, authorize, current_state_query, history_query, maintenance_ddl, object_fingerprint,
};

fn main() {
    assert!(maintenance_ddl(Principal::Runtime).is_err());
    assert!(
        authorize(
            Principal::Runtime,
            maintenance_ddl(Principal::Maintenance).unwrap()
        )
        .is_err()
    );
    assert!(authorize(Principal::Runtime, history_query()).is_ok());
    assert!(authorize(Principal::Runtime, current_state_query()).is_ok());
    println!("{}", object_fingerprint());
}
