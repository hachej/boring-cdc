\set ON_ERROR_STOP on
CREATE ROLE boring_cdc_capture_bootstrap LOGIN REPLICATION PASSWORD 'capture_fixture_only';
CREATE ROLE boring_cdc_control_writer LOGIN PASSWORD 'control_fixture_only';
CREATE ROLE boring_cdc_application LOGIN PASSWORD 'application_fixture_only';
CREATE ROLE boring_cdc_admin NOLOGIN;
CREATE SCHEMA boring_cdc_control;
CREATE TABLE public.accounts(id bigint PRIMARY KEY, value text);
CREATE TABLE boring_cdc_control.heartbeat(id text PRIMARY KEY CHECK (id = 'singleton'), nonce bigint NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE boring_cdc_control.capture_fences(id text PRIMARY KEY CHECK (id = 'singleton'), capture_epoch bigint NOT NULL, generation bigint NOT NULL, table_set_fingerprint text NOT NULL, unique_nonce bigint NOT NULL);
INSERT INTO boring_cdc_control.heartbeat VALUES ('singleton', 0, now());
INSERT INTO boring_cdc_control.capture_fences VALUES ('singleton', 0, 0, repeat('0',64), 0);
GRANT USAGE ON SCHEMA boring_cdc_control TO boring_cdc_control_writer;
GRANT SELECT(id), UPDATE(nonce, updated_at) ON boring_cdc_control.heartbeat TO boring_cdc_control_writer;
GRANT SELECT(id), UPDATE(capture_epoch, generation, table_set_fingerprint, unique_nonce) ON boring_cdc_control.capture_fences TO boring_cdc_control_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.accounts TO boring_cdc_application;
CREATE PUBLICATION boring_cdc_publication FOR TABLE public.accounts, boring_cdc_control.heartbeat, boring_cdc_control.capture_fences WITH (publish='insert,update,delete,truncate');
ALTER PUBLICATION boring_cdc_publication OWNER TO boring_cdc_admin;
