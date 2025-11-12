-- Test lockstep index scan optimization for array-based lookups (IN clauses)
-- This test validates that queries with IN clauses use lockstep scanning
-- instead of performing separate B-tree descents for each array element

CREATE EXTENSION IF NOT EXISTS orioledb;

-- Setup: Create a table with indexed column for IN clause testing
CREATE TABLE o_lockstep_test (
	id int4 NOT NULL,
	value text NOT NULL,
	data int4,
	PRIMARY KEY (id)
) USING orioledb;

-- Insert test data with gaps to test lockstep scanning behavior
INSERT INTO o_lockstep_test 
	SELECT i, 'value_' || i, i * 10
	FROM generate_series(1, 100) AS i;

ANALYZE o_lockstep_test;

SET enable_seqscan = off;

-- Test 1: Basic IN clause with sorted values
EXPLAIN (COSTS off) SELECT id, value FROM o_lockstep_test WHERE id IN (5, 10, 15, 20, 25);
SELECT id, value FROM o_lockstep_test WHERE id IN (5, 10, 15, 20, 25);

-- Test 2: IN clause with unsorted values (PostgreSQL sorts them internally)
EXPLAIN (COSTS off) SELECT id, value FROM o_lockstep_test WHERE id IN (25, 10, 5, 20, 15);
SELECT id, value FROM o_lockstep_test WHERE id IN (25, 10, 5, 20, 15);

-- Test 3: Large IN list to stress test lockstep scanning
EXPLAIN (COSTS off) SELECT id FROM o_lockstep_test WHERE id IN (1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100);
SELECT id FROM o_lockstep_test WHERE id IN (1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100);

-- Test 4: IN clause with values not in table
EXPLAIN (COSTS off) SELECT id FROM o_lockstep_test WHERE id IN (101, 102, 103, 104, 105);
SELECT id FROM o_lockstep_test WHERE id IN (101, 102, 103, 104, 105);

-- Test 5: IN clause with mix of existing and non-existing values
EXPLAIN (COSTS off) SELECT id FROM o_lockstep_test WHERE id IN (5, 999, 10, 888, 15);
SELECT id FROM o_lockstep_test WHERE id IN (5, 999, 10, 888, 15);

-- Setup: Create a composite index for multi-column IN testing
CREATE TABLE o_lockstep_composite (
	col1 int4 NOT NULL,
	col2 int4 NOT NULL,
	col3 text,
	PRIMARY KEY (col1, col2)
) USING orioledb;

INSERT INTO o_lockstep_composite
	SELECT i, j, 'data_' || i || '_' || j
	FROM generate_series(1, 10) AS i,
	     generate_series(1, 10) AS j;

ANALYZE o_lockstep_composite;

-- Test 6: IN clause on first column of composite index
EXPLAIN (COSTS off) SELECT col1, col2 FROM o_lockstep_composite WHERE col1 IN (2, 4, 6, 8);
SELECT col1, col2 FROM o_lockstep_composite WHERE col1 IN (2, 4, 6, 8) ORDER BY col1, col2;

-- Test 7: IN clause on both columns of composite index
EXPLAIN (COSTS off) SELECT col1, col2 FROM o_lockstep_composite WHERE col1 IN (2, 4) AND col2 IN (3, 5, 7);
SELECT col1, col2 FROM o_lockstep_composite WHERE col1 IN (2, 4) AND col2 IN (3, 5, 7) ORDER BY col1, col2;

-- Test 8: Combined IN clause with range predicate
EXPLAIN (COSTS off) SELECT col1, col2 FROM o_lockstep_composite WHERE col1 IN (2, 4, 6) AND col2 > 5;
SELECT col1, col2 FROM o_lockstep_composite WHERE col1 IN (2, 4, 6) AND col2 > 5 ORDER BY col1, col2;

-- Setup: Create a secondary index test
CREATE TABLE o_lockstep_secondary (
	id int4 NOT NULL,
	category int4 NOT NULL,
	value text,
	PRIMARY KEY (id)
) USING orioledb;

CREATE INDEX o_lockstep_secondary_cat ON o_lockstep_secondary(category);

INSERT INTO o_lockstep_secondary
	SELECT i, (i % 10) + 1, 'item_' || i
	FROM generate_series(1, 50) AS i;

ANALYZE o_lockstep_secondary;

-- Test 9: IN clause on secondary index
EXPLAIN (COSTS off) SELECT id, category FROM o_lockstep_secondary WHERE category IN (2, 4, 6, 8);
SELECT id, category FROM o_lockstep_secondary WHERE category IN (2, 4, 6, 8) ORDER BY id;

-- Test 10: Verify correctness with ORDER BY
EXPLAIN (COSTS off) SELECT id FROM o_lockstep_test WHERE id IN (10, 20, 30, 40, 50) ORDER BY id;
SELECT id FROM o_lockstep_test WHERE id IN (10, 20, 30, 40, 50) ORDER BY id;

EXPLAIN (COSTS off) SELECT id FROM o_lockstep_test WHERE id IN (10, 20, 30, 40, 50) ORDER BY id DESC;
SELECT id FROM o_lockstep_test WHERE id IN (10, 20, 30, 40, 50) ORDER BY id DESC;

-- Cleanup
DROP TABLE o_lockstep_test;
DROP TABLE o_lockstep_composite;
DROP TABLE o_lockstep_secondary;
