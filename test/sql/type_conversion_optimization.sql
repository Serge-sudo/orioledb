--
-- Test type conversion optimization in o_fill_key_bounds
-- This test verifies that scan keys are converted to index field types
-- when possible, enabling optimizations for queries like BETWEEN with
-- literal values.
--

CREATE SCHEMA type_conv_opt;
SET SESSION search_path = 'type_conv_opt';
CREATE EXTENSION orioledb;

-- Test int8 to int4 conversion
CREATE TABLE o_test_int4
(
	id int4 PRIMARY KEY,
	value text
) USING orioledb;

INSERT INTO o_test_int4 SELECT i, 'value_' || i FROM generate_series(1, 100) AS i;

-- These queries use int8 literals (1, 10) with int4 column
-- The optimization should convert them to int4 without overflow
EXPLAIN (COSTS off) SELECT * FROM o_test_int4 WHERE id BETWEEN 1 AND 10;
SELECT * FROM o_test_int4 WHERE id BETWEEN 1 AND 10;

EXPLAIN (COSTS off) SELECT * FROM o_test_int4 WHERE id = 5;
SELECT * FROM o_test_int4 WHERE id = 5;

-- Test with values near int4 boundaries (should still work)
EXPLAIN (COSTS off) SELECT * FROM o_test_int4 WHERE id BETWEEN 1 AND 100;
SELECT count(*) FROM o_test_int4 WHERE id BETWEEN 1 AND 100;

-- Test int8 to int2 conversion
CREATE TABLE o_test_int2
(
	id int2 PRIMARY KEY,
	value text
) USING orioledb;

INSERT INTO o_test_int2 SELECT i, 'value_' || i FROM generate_series(1, 100) AS i;

-- These queries use int8 literals with int2 column
EXPLAIN (COSTS off) SELECT * FROM o_test_int2 WHERE id BETWEEN 1 AND 10;
SELECT * FROM o_test_int2 WHERE id BETWEEN 1 AND 10;

EXPLAIN (COSTS off) SELECT * FROM o_test_int2 WHERE id = 5;
SELECT * FROM o_test_int2 WHERE id = 5;

-- Test int4 to int2 conversion
EXPLAIN (COSTS off) SELECT * FROM o_test_int2 WHERE id BETWEEN 1::int4 AND 10::int4;
SELECT * FROM o_test_int2 WHERE id BETWEEN 1::int4 AND 10::int4;

-- Test float8 to float4 conversion
CREATE TABLE o_test_float4
(
	id int4 PRIMARY KEY,
	value float4
) USING orioledb;

INSERT INTO o_test_float4 SELECT i, i * 1.5 FROM generate_series(1, 100) AS i;

-- Create index on float4 column
CREATE INDEX o_test_float4_value_idx ON o_test_float4(value);

-- Query with float8 literals on float4 column
EXPLAIN (COSTS off) SELECT * FROM o_test_float4 WHERE value BETWEEN 1.5 AND 15.0;
SELECT * FROM o_test_float4 WHERE value BETWEEN 1.5 AND 15.0;

-- Test with mixed type columns
CREATE TABLE o_test_mixed
(
	id_int2 int2,
	id_int4 int4,
	id_int8 int8,
	val_float4 float4,
	val_float8 float8,
	PRIMARY KEY(id_int4)
) USING orioledb;

INSERT INTO o_test_mixed 
SELECT i::int2, i, i::int8, i * 1.1, i * 2.2 
FROM generate_series(1, 50) AS i;

-- Test with int8 literals on int4 primary key
EXPLAIN (COSTS off) SELECT * FROM o_test_mixed WHERE id_int4 BETWEEN 1 AND 10;
SELECT * FROM o_test_mixed WHERE id_int4 BETWEEN 1 AND 10;

-- Create secondary indexes with different types
CREATE INDEX o_test_mixed_int2_idx ON o_test_mixed(id_int2);
CREATE INDEX o_test_mixed_float4_idx ON o_test_mixed(val_float4);

-- Test with int8 literal on int2 secondary index
EXPLAIN (COSTS off) SELECT id_int2 FROM o_test_mixed WHERE id_int2 BETWEEN 5 AND 15;
SELECT id_int2 FROM o_test_mixed WHERE id_int2 BETWEEN 5 AND 15;

-- Test with float8 literal on float4 secondary index
EXPLAIN (COSTS off) SELECT val_float4 FROM o_test_mixed WHERE val_float4 BETWEEN 5.5 AND 16.5;
SELECT val_float4 FROM o_test_mixed WHERE val_float4 BETWEEN 5.5 AND 16.5;

-- Test that overflow is handled correctly (no conversion should happen)
-- These should still work but may use a different code path
CREATE TABLE o_test_overflow
(
	id int4 PRIMARY KEY,
	value text
) USING orioledb;

INSERT INTO o_test_overflow SELECT i, 'value_' || i FROM generate_series(1, 10) AS i;

-- Query with value that would overflow int4 - should still work correctly
-- (9223372036854775807 is max int8, much larger than int4 max)
EXPLAIN (COSTS off) SELECT * FROM o_test_overflow WHERE id < 9223372036854775807;
SELECT * FROM o_test_overflow WHERE id < 9223372036854775807;

-- Test with IN clause (uses array)
EXPLAIN (COSTS off) SELECT * FROM o_test_int4 WHERE id IN (1, 5, 10, 20);
SELECT * FROM o_test_int4 WHERE id IN (1, 5, 10, 20);

-- Cleanup
DROP TABLE o_test_int4;
DROP TABLE o_test_int2;
DROP TABLE o_test_float4;
DROP TABLE o_test_mixed;
DROP TABLE o_test_overflow;

DROP SCHEMA type_conv_opt CASCADE;
RESET search_path;
