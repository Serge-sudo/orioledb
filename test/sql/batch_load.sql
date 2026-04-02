CREATE EXTENSION orioledb;

-- test 1: ctid
CREATE TABLE o_test(
	id integer NOT NULL,
	val text NOT NULL
) USING orioledb;

CREATE TABLE o_test_n(
	id integer NOT NULL,
	val text NOT NULL
);

\timing
COPY o_test FROM '/tmp/o_test_unsort.csv' WITH (FORMAT csv);
COPY o_test_n FROM '/tmp/o_test_unsort.csv' WITH (FORMAT csv);
\timing

SELECT COUNT(*) FROM o_test;
SELECT COUNT(*) FROM o_test_n;

DROP TABLE o_test;
DROP TABLE o_test_n;

-- test 2: sorted input with pk
-- CREATE TABLE o_test(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- ) USING orioledb;

-- CREATE TABLE o_test_n(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- );

-- \timing
-- COPY o_test FROM '/tmp/o_test_sort.csv' WITH (FORMAT csv);
-- COPY o_test_n FROM '/tmp/o_test_sort.csv' WITH (FORMAT csv);
-- \timing

-- SELECT COUNT(*) FROM o_test;
-- SELECT COUNT(*) FROM o_test_n;

-- DROP TABLE o_test;
-- DROP TABLE o_test_n;

-- -- test 3: unsorted input with pk
-- CREATE TABLE o_test(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- ) USING orioledb;

-- CREATE TABLE o_test_n(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- );

-- \timing
-- COPY o_test FROM '/tmp/o_test_unsort.csv' WITH (FORMAT csv);
-- COPY o_test_n FROM '/tmp/o_test_unsort.csv' WITH (FORMAT csv);
-- \timing

-- SELECT COUNT(*) FROM o_test;
-- SELECT COUNT(*) FROM o_test_n;
-- DROP TABLE o_test;

-- -- test 4:

-- CREATE TABLE o_test(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- ) USING orioledb;

-- INSERT INTO o_test SELECT i, 'val' || i FROM generate_series(1, 170) s(i) where i % 3 = 0;
-- SELECT orioledb_idx_structure('o_test'::regclass, 'o_test_pkey', 'nue', 3);
-- COPY o_test FROM '/tmp/o_test_hole.csv' WITH (FORMAT csv);
-- SELECT orioledb_idx_structure('o_test'::regclass, 'o_test_pkey', 'nue', 3);
-- SELECT COUNT(*) FROM o_test;
-- DROP TABLE o_test;

-- -- test 5:
-- CREATE TABLE o_test(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- ) USING orioledb;

-- INSERT INTO o_test SELECT i, 'val' || i FROM generate_series(1, 170) s(i);
-- DELETE FROM o_test WHERE id % 3 != 0;
-- SELECT orioledb_idx_structure('o_test'::regclass, 'o_test_pkey', 'nue', 3);
-- COPY o_test FROM '/tmp/o_test_hole.csv' WITH (FORMAT csv);
-- SELECT orioledb_idx_structure('o_test'::regclass, 'o_test_pkey', 'nue', 3);
-- SELECT COUNT(*) FROM o_test;
-- DROP TABLE o_test;

-- -- test 6: unqiue
-- CREATE TABLE o_test(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- ) USING orioledb;

-- INSERT INTO o_test SELECT i, 'val' || i FROM generate_series(1, 170) s(i);
-- SELECT COUNT(*) FROM o_test;
-- COPY o_test FROM '/tmp/o_test_hole.csv' WITH (FORMAT csv);
-- SELECT COUNT(*) FROM o_test;
-- DELETE FROM o_test WHERE id = 1;
-- COPY o_test FROM '/tmp/o_test_hole.csv' WITH (FORMAT csv);
-- SELECT COUNT(*) FROM o_test;
-- DROP TABLE o_test;

-- -- test 7: undo
-- CREATE TABLE o_test(
-- 	id integer NOT NULL,
-- 	val text NOT NULL,
-- 	PRIMARY KEY (id)
-- ) USING orioledb;

-- BEGIN;
-- SELECT COUNT(*) FROM o_test;
-- COPY o_test FROM '/tmp/o_test_hole.csv' WITH (FORMAT csv);
-- SELECT COUNT(*) FROM o_test;
-- SELECT orioledb_idx_structure('o_test'::regclass, 'o_test_pkey', 'nue', 3);
-- ROLLBACK;

-- SELECT orioledb_idx_structure('o_test'::regclass, 'o_test_pkey', 'nue', 3);
-- SELECT COUNT(*) FROM o_test;
-- DROP TABLE o_test;

DROP EXTENSION orioledb CASCADE;


-- INSERT INTO o_test SELECT i, 'val' || i FROM generate_series(1, 10000000) s(i);
-- COPY o_test TO '/tmp/o_test.csv' WITH (FORMAT csv);
-- TRUNCATE o_test;