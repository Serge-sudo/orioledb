/* contrib/orioledb/sql/orioledb--1.6--1.7_dev.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.7'" to load this file. \quit

CREATE FUNCTION orioledb_predicate_locks(OUT datoid oid, OUT reloid oid, OUT relnode oid,
                                         OUT pid int4, OUT oxid int8, OUT lock_level text,
                                         OUT key jsonb, OUT lokey jsonb,
                                         OUT key_raw bytea, OUT key_format_flags int2,
                                         OUT lokey_raw bytea, OUT lokey_format_flags int2)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
