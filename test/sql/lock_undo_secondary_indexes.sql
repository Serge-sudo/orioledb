-- Test to demonstrate that lock undo only occurs on primary indexes, not secondary indexes
-- This test verifies the architectural design where row locks are applied only to the primary index

CREATE SCHEMA lock_undo_test;
SET SESSION search_path = 'lock_undo_test';
CREATE EXTENSION orioledb;

-- Create a table with a primary key and a secondary index
CREATE TABLE test_table
(
    id int8 PRIMARY KEY,
    value int8 NOT NULL,
    indexed_col int8 NOT NULL
) USING orioledb;

-- Create a secondary index
CREATE INDEX idx_indexed_col ON test_table(indexed_col);

-- Insert test data
INSERT INTO test_table VALUES (1, 100, 1000);
INSERT INTO test_table VALUES (2, 200, 2000);
INSERT INTO test_table VALUES (3, 300, 3000);

-- Test 1: Verify that SELECT FOR SHARE locks the row via the primary index
BEGIN;
SELECT * FROM test_table WHERE id = 1 FOR SHARE;

-- The orioledb_tbl_structure should show the lock in the primary index
-- This demonstrates that the lock is on the primary index
SELECT orioledb_tbl_structure('test_table'::regclass, 'ne');

ROLLBACK;

-- Test 2: Verify that accessing via secondary index still locks via primary index
BEGIN;
SELECT * FROM test_table WHERE indexed_col = 1000 FOR SHARE;

-- Even though we accessed via the secondary index, the lock is still on the primary index
SELECT orioledb_tbl_structure('test_table'::regclass, 'ne');

ROLLBACK;

-- Test 3: Demonstrate lock undo on primary index during rollback
BEGIN;
-- Lock the row
SELECT * FROM test_table WHERE id = 1 FOR SHARE;
SELECT orioledb_tbl_structure('test_table'::regclass, 'ne');

SAVEPOINT s1;
-- Upgrade to FOR UPDATE lock
SELECT * FROM test_table WHERE id = 1 FOR UPDATE;
SELECT orioledb_tbl_structure('test_table'::regclass, 'ne');

-- Rollback should undo the FOR UPDATE lock via lock undo
ROLLBACK TO s1;
SELECT orioledb_tbl_structure('test_table'::regclass, 'ne');

ROLLBACK;

-- Test 4: Verify secondary index operations use modify undo, not lock undo
BEGIN;
-- Update that affects the secondary index
UPDATE test_table SET indexed_col = 1001 WHERE id = 1;

-- Check structure - this should show modify operations on both primary and secondary index
SELECT orioledb_tbl_structure('test_table'::regclass, 'ne');

ROLLBACK;

-- Verify data is back to original after rollback
SELECT * FROM test_table ORDER BY id;

-- Test 5: Concurrent access test (requires manual verification in separate sessions)
-- Session 1: BEGIN; SELECT * FROM test_table WHERE id = 1 FOR UPDATE;
-- Session 2: Should block on: SELECT * FROM test_table WHERE indexed_col = 1000 FOR UPDATE;
-- This demonstrates that the lock on the primary index protects access via secondary index

-- Clean up
DROP TABLE test_table CASCADE;
DROP SCHEMA lock_undo_test CASCADE;
