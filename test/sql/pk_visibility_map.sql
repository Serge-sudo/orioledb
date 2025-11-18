-- Test PK-based visibility map for OrioleDB tables
-- This tests the new PK-range-based VM that enables index-only scans
-- without checking the primary index for visibility

CREATE SCHEMA pk_visibility_map;
SET SESSION search_path = 'pk_visibility_map';
CREATE EXTENSION orioledb;

-- Create a simple table with a secondary index
CREATE TABLE o_test_vm
(
	id int PRIMARY KEY,
	val int,
	data text
) USING orioledb;

-- Create a secondary index on val
CREATE INDEX o_test_vm_val_idx ON o_test_vm(val);

-- Insert some data
INSERT INTO o_test_vm SELECT i, i * 10, 'data_' || i FROM generate_series(1, 1000) i;

-- Run ANALYZE to build the visibility map
ANALYZE o_test_vm;

-- Check that relallvisible is set correctly in pg_class
-- The VM should report pages as all-visible
SELECT c.relname, c.relpages > 0 as has_pages, c.relallvisible > 0 as has_allvisible
FROM pg_class c
WHERE c.relname = 'o_test_vm';

-- Test 1: Index-only scan on secondary index
-- This should use Index Only Scan and the VM should indicate visibility
SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS OFF) SELECT val FROM o_test_vm WHERE val > 500 AND val < 600 ORDER BY val LIMIT 10;
SELECT val FROM o_test_vm WHERE val > 500 AND val < 600 ORDER BY val LIMIT 10;

-- Test 2: Covering index with INCLUDE
CREATE INDEX o_test_vm_val_id_idx ON o_test_vm(val) INCLUDE (id);

EXPLAIN (COSTS OFF) SELECT val, id FROM o_test_vm WHERE val > 500 AND val < 600 ORDER BY val LIMIT 10;
SELECT val, id FROM o_test_vm WHERE val > 500 AND val < 600 ORDER BY val LIMIT 10;

-- Test 3: After VACUUM, VM should be rebuilt
VACUUM o_test_vm;

-- Check that relallvisible is maintained after VACUUM
SELECT c.relname, c.relpages > 0 as has_pages, c.relallvisible > 0 as has_allvisible
FROM pg_class c
WHERE c.relname = 'o_test_vm';

-- Test 4: Insert more data and re-analyze
INSERT INTO o_test_vm SELECT i, i * 10, 'data_' || i FROM generate_series(1001, 2000) i;
ANALYZE o_test_vm;

-- VM should be updated to include new PK ranges
SELECT c.relname, c.relpages, c.relallvisible, 
       CASE WHEN c.relallvisible = c.relpages THEN 'all_visible' ELSE 'partial' END as status
FROM pg_class c
WHERE c.relname = 'o_test_vm';

-- Index-only scan should still work with expanded data
EXPLAIN (COSTS OFF) SELECT val FROM o_test_vm WHERE val > 1500 ORDER BY val LIMIT 10;
SELECT val FROM o_test_vm WHERE val > 1500 ORDER BY val LIMIT 10;

-- Clean up
DROP SCHEMA pk_visibility_map CASCADE;
