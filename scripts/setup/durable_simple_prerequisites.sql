\set ON_ERROR_STOP on

-- Run as the database owner (or a superuser) against a newly-created database.
-- Required variables are supplied with psql -v; psql safely quotes every password.
\if :{?admin_password}
\else
  \echo 'admin_password is required'
  \quit 2
\endif
\if :{?runtime_password}
\else
  \echo 'runtime_password is required'
  \quit 2
\endif
\if :{?control_password}
\else
  \echo 'control_password is required'
  \quit 2
\endif
\if :{?application_password}
\else
  \echo 'application_password is required'
  \quit 2
\endif

SELECT format('CREATE ROLE boring_cdc_admin LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'admin_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'boring_cdc_admin') \gexec
SELECT format('ALTER ROLE boring_cdc_admin LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'admin_password') \gexec

SELECT format('CREATE ROLE boring_cdc_runtime LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS', :'runtime_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'boring_cdc_runtime') \gexec
SELECT format('ALTER ROLE boring_cdc_runtime LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS', :'runtime_password') \gexec

SELECT format('CREATE ROLE boring_cdc_control_writer LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'control_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'boring_cdc_control_writer') \gexec
SELECT format('ALTER ROLE boring_cdc_control_writer LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'control_password') \gexec

SELECT format('CREATE ROLE boring_cdc_app LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'application_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'boring_cdc_app') \gexec
SELECT format('ALTER ROLE boring_cdc_app LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', :'application_password') \gexec

-- init connects as boring_cdc_admin. It creates/owns the control schema, its two
-- fixed rows, and the configured publication; it never creates the logical slot.
GRANT CONNECT, CREATE ON DATABASE :"DBNAME" TO boring_cdc_admin;
GRANT CONNECT ON DATABASE :"DBNAME" TO boring_cdc_runtime, boring_cdc_control_writer, boring_cdc_app;

CREATE TABLE IF NOT EXISTS public.orders (
    id bigint PRIMARY KEY,
    total numeric
);
ALTER TABLE public.orders OWNER TO boring_cdc_admin;
GRANT SELECT ON public.orders TO boring_cdc_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.orders TO boring_cdc_app;

-- init grants the control writer only SELECT(id) and UPDATE(value columns) on
-- boring_cdc_control.heartbeat and boring_cdc_control.capture_fences.
-- Do not grant these roles membership in one another or table-wide control access.
