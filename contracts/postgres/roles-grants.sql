-- Boring CDC v0.1 source role/grant contract. Administrative execution substitutes
-- safely quoted configured identifiers; ordinary run never receives the admin credential.
CREATE ROLE boring_cdc_admin NOLOGIN;
CREATE ROLE boring_cdc_application NOLOGIN;
CREATE ROLE boring_cdc_capture LOGIN REPLICATION;
CREATE ROLE boring_cdc_control_writer LOGIN;
-- The administrator grants CONNECT on the configured database and USAGE/SELECT on
-- each admitted selected schema/table to boring_cdc_capture. No INSERT/UPDATE/DELETE
-- grant is issued to that role. Application-table DML belongs only to application roles.

CREATE SCHEMA boring_cdc_control AUTHORIZATION boring_cdc_admin;
CREATE TABLE boring_cdc_control.heartbeat (
  id smallint PRIMARY KEY CHECK (id = 1),
  nonce bigint NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE TABLE boring_cdc_control.capture_fences (
  id smallint PRIMARY KEY CHECK (id = 1),
  capture_epoch bigint NOT NULL,
  generation bigint NOT NULL,
  table_set_fingerprint bytea NOT NULL CHECK (octet_length(table_set_fingerprint) = 32),
  unique_nonce bytea NOT NULL CHECK (octet_length(unique_nonce) = 16)
);
INSERT INTO boring_cdc_control.heartbeat VALUES (1, 0, '-infinity');
INSERT INTO boring_cdc_control.capture_fences VALUES (1, 0, 0, decode(repeat('00',32),'hex'), decode(repeat('00',16),'hex'));
REVOKE ALL ON SCHEMA boring_cdc_control FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA boring_cdc_control FROM PUBLIC;
GRANT USAGE ON SCHEMA boring_cdc_control TO boring_cdc_capture, boring_cdc_control_writer;
GRANT SELECT (id) ON boring_cdc_control.heartbeat, boring_cdc_control.capture_fences TO boring_cdc_control_writer;
GRANT UPDATE (nonce, updated_at) ON boring_cdc_control.heartbeat TO boring_cdc_control_writer;
GRANT UPDATE (capture_epoch, generation, table_set_fingerprint, unique_nonce) ON boring_cdc_control.capture_fences TO boring_cdc_control_writer;
GRANT SELECT ON boring_cdc_control.heartbeat, boring_cdc_control.capture_fences TO boring_cdc_capture;
ALTER TABLE boring_cdc_control.heartbeat OWNER TO boring_cdc_admin;
ALTER TABLE boring_cdc_control.capture_fences OWNER TO boring_cdc_admin;
CREATE PUBLICATION boring_cdc FOR TABLE boring_cdc_control.heartbeat, boring_cdc_control.capture_fences
  WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = false);
ALTER PUBLICATION boring_cdc OWNER TO boring_cdc_admin;
-- PostgreSQL cannot scope REPLICATION to a slot name or deny DROP_REPLICATION_SLOT:
-- connector code must require both locks plus a matching persisted intent, allow only the
-- configured pgoutput slot creation with EXPORT_SNAPSHOT, and expose no runtime drop path.

-- The maintenance owner calls this once for the exact nonempty preflighted selected-table set.
-- regclass rendering is identifier-quoted by PostgreSQL; duplicate membership fails closed.
CREATE FUNCTION boring_cdc_control.configure_selected_relations(selected regclass[])
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE relation regclass;
BEGIN
  IF selected IS NULL OR cardinality(selected) = 0 THEN
    RAISE EXCEPTION 'selected relation set must be nonempty';
  END IF;
  FOREACH relation IN ARRAY selected LOOP
    EXECUTE format('GRANT SELECT ON TABLE %s TO boring_cdc_capture', relation);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO boring_cdc_capture',
      (SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=relation));
    EXECUTE format('ALTER PUBLICATION boring_cdc ADD TABLE %s', relation);
  END LOOP;
END $$;
ALTER FUNCTION boring_cdc_control.configure_selected_relations(regclass[]) OWNER TO boring_cdc_admin;
REVOKE ALL ON FUNCTION boring_cdc_control.configure_selected_relations(regclass[]) FROM PUBLIC;
-- Example maintenance invocation after exact preflight:
-- SELECT boring_cdc_control.configure_selected_relations(ARRAY['public.orders'::regclass]);
-- DROP FUNCTION boring_cdc_control.configure_selected_relations(regclass[]);
