use boring_cdc::m2_schema::open_writer;
use std::path::Path;
fn main() {
    let p = std::env::args().nth(1).expect("sqlite path");
    let w = open_writer(Path::new(&p), "fault-status-fixture", 1, 0).expect("schema");
    w.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint,control_revision) VALUES(1,'epoch-status','source-secret','timeline','database-secret','slot-secret','pgoutput','publication-fingerprint','protocol-fingerprint',2)",[]).expect("source");
    w.connection().execute("INSERT INTO startup_reconciliations(run_id,capture_epoch,outcome,reason_code,created_at) VALUES('status-run','epoch-status','bootstrap_ambiguous_requires_restart','BOOTSTRAP_PROVENANCE_AMBIGUOUS','fixture')",[]).expect("receipt");
}
