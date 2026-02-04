-- Tests for concurrent secondary index build
-- Currently concurrent index builds are not supported, but this file 
-- documents the expected behavior and will be extended when the 
-- feature is implemented using the PK undo-based approach.

CREATE SCHEMA concurrent_index_test;
SET SESSION search_path = 'concurrent_index_test';
CREATE EXTENSION orioledb;

-- Create a test table
CREATE TABLE o_concurrent_test (
    id int8 NOT NULL PRIMARY KEY,
    value text,
    extra int
) USING orioledb;

-- Insert some initial data
INSERT INTO o_concurrent_test 
SELECT id, 'value_' || id, id * 10 
FROM generate_series(1, 100) as id;

-- Verify the data
SELECT COUNT(*) FROM o_concurrent_test;
SELECT * FROM o_concurrent_test WHERE id <= 5 ORDER BY id;

-- Test: CREATE INDEX CONCURRENTLY is not yet supported
-- This should produce an error
CREATE INDEX CONCURRENTLY o_concurrent_ix1 ON o_concurrent_test (value);

-- Create a regular index (non-concurrent) - this should work
CREATE INDEX o_concurrent_ix2 ON o_concurrent_test (value);

-- Verify the index was created
SELECT orioledb_tbl_indices('o_concurrent_test'::regclass);

-- Test that the index works
SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_concurrent_test WHERE value = 'value_50';
SELECT * FROM o_concurrent_test WHERE value = 'value_50';
RESET enable_seqscan;

-- Test index behavior with INSERT/UPDATE/DELETE
INSERT INTO o_concurrent_test VALUES (101, 'value_101', 1010);
UPDATE o_concurrent_test SET value = 'updated_value' WHERE id = 50;
DELETE FROM o_concurrent_test WHERE id = 100;

-- Verify the changes
SELECT * FROM o_concurrent_test WHERE id IN (50, 100, 101) ORDER BY id;

-- Test with transactions
BEGIN;
INSERT INTO o_concurrent_test VALUES (102, 'value_102', 1020);
SAVEPOINT sp1;
UPDATE o_concurrent_test SET value = 'temp_value' WHERE id = 102;
ROLLBACK TO SAVEPOINT sp1;
-- After rollback to savepoint, the value should be 'value_102'
SELECT * FROM o_concurrent_test WHERE id = 102;
COMMIT;

-- Verify final state
SELECT * FROM o_concurrent_test WHERE id = 102;

-- Clean up
DROP TABLE o_concurrent_test;
DROP SCHEMA concurrent_index_test CASCADE;
RESET search_path;
