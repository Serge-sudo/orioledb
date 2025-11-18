-- Test that index-only scans work correctly with OrioleDB tables
-- This verifies the fix for relallvisible being 0

CREATE SCHEMA index_only_scan;
SET SESSION search_path = 'index_only_scan';
CREATE EXTENSION orioledb;

-- Create a simple table with an index
CREATE TABLE o_test_ios
(
	id int PRIMARY KEY,
	val int,
	data text
) USING orioledb;

-- Create an index on val
CREATE INDEX o_test_ios_val_idx ON o_test_ios(val);

-- Insert some data
INSERT INTO o_test_ios SELECT i, i * 10, 'data_' || i FROM generate_series(1, 1000) i;

-- Run ANALYZE to update statistics
ANALYZE o_test_ios;

-- Check that relallvisible is set correctly in pg_class
-- After our fix, relallvisible should be equal to relpages for OrioleDB tables
SELECT c.relname, c.relpages > 0 as has_pages, c.relallvisible > 0 as has_allvisible
FROM pg_class c
WHERE c.relname = 'o_test_ios';

-- Test 1: Simple index-only scan selecting only indexed column
-- This should use Index Only Scan because val is in the index
SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS OFF) SELECT val FROM o_test_ios WHERE val > 500 ORDER BY val LIMIT 10;
SELECT val FROM o_test_ios WHERE val > 500 ORDER BY val LIMIT 10;

-- Test 2: Query selecting both indexed and non-indexed columns
-- This should use Index Scan (not index-only) because data is not in index
EXPLAIN (COSTS OFF) SELECT val, data FROM o_test_ios WHERE val > 500 ORDER BY val LIMIT 10;
SELECT val, data FROM o_test_ios WHERE val > 500 ORDER BY val LIMIT 10;

-- Test 3: Covering index (index includes all columns needed)
-- Create a covering index
CREATE INDEX o_test_ios_val_id_idx ON o_test_ios(val) INCLUDE (id);

-- This should use Index Only Scan because both val and id are in the covering index
EXPLAIN (COSTS OFF) SELECT val, id FROM o_test_ios WHERE val > 500 ORDER BY val LIMIT 10;
SELECT val, id FROM o_test_ios WHERE val > 500 ORDER BY val LIMIT 10;

-- Test 4: Verify behavior with table that has bridged indexes (requires index_bridging option)
-- First, create a table with index bridging enabled
CREATE TABLE o_test_ios_bridged
(
	id int PRIMARY KEY,
	val int,
	data text
) USING orioledb WITH (index_bridging = true);

CREATE INDEX o_test_ios_bridged_val_idx ON o_test_ios_bridged(val);
INSERT INTO o_test_ios_bridged SELECT i, i * 10, 'data_' || i FROM generate_series(1, 1000) i;

-- Run VACUUM to trigger statistics update for tables with bridged indexes
VACUUM o_test_ios_bridged;

-- Check that relallvisible is set correctly after VACUUM
SELECT c.relname, c.relpages > 0 as has_pages, c.relallvisible > 0 as has_allvisible,
       c.relallvisible = c.relpages as allvisible_equals_pages
FROM pg_class c
WHERE c.relname = 'o_test_ios_bridged';

-- The query should use Index Only Scan
EXPLAIN (COSTS OFF) SELECT val FROM o_test_ios_bridged WHERE val > 500 ORDER BY val LIMIT 10;

-- Test 5: Compare with regular heap table to demonstrate the difference
CREATE TABLE heap_test_ios
(
	id int PRIMARY KEY,
	val int,
	data text
);

CREATE INDEX heap_test_ios_val_idx ON heap_test_ios(val);

INSERT INTO heap_test_ios SELECT i, i * 10, 'data_' || i FROM generate_series(1, 1000) i;
ANALYZE heap_test_ios;

-- Check relallvisible for heap table (should be 0 before VACUUM)
SELECT c.relname, c.relpages > 0 as has_pages, c.relallvisible
FROM pg_class c
WHERE c.relname = 'heap_test_ios';

-- Even though both tables have similar data, the planner behavior differs
-- OrioleDB should favor index-only scans more readily
EXPLAIN (COSTS OFF) SELECT val FROM heap_test_ios WHERE val > 500 ORDER BY val LIMIT 10;

-- Clean up
DROP SCHEMA index_only_scan CASCADE;
