-- Test CREATE INDEX CONCURRENTLY functionality
CREATE SCHEMA concurrent_index;
SET SESSION search_path = 'concurrent_index';
CREATE EXTENSION orioledb;

-- Create a table with some data
CREATE TABLE o_test_concurrent
(
	id bigint NOT NULL,
	data text,
	val int,
	PRIMARY KEY (id)
) USING orioledb;

-- Insert initial data
INSERT INTO o_test_concurrent 
SELECT i, 'data_' || i, i * 10 
FROM generate_series(1, 1000) AS i;

-- Test 1: Basic concurrent index creation
CREATE INDEX CONCURRENTLY o_test_concurrent_val_idx ON o_test_concurrent(val);

-- Verify index was created and is usable
SELECT indexname FROM pg_indexes WHERE tablename = 'o_test_concurrent' ORDER BY indexname;

SET enable_seqscan = off;
EXPLAIN (COSTS off) SELECT * FROM o_test_concurrent WHERE val < 100 ORDER BY val;

-- Verify results using the index
SELECT count(*) FROM o_test_concurrent WHERE val < 100;

-- Test 2: Concurrent index with predicate
CREATE INDEX CONCURRENTLY o_test_concurrent_partial_idx 
ON o_test_concurrent(data) WHERE val > 500;

SELECT indexname FROM pg_indexes WHERE tablename = 'o_test_concurrent' ORDER BY indexname;

-- Test 3: Verify index contents are correct
SELECT count(*) FROM o_test_concurrent WHERE val < 100;
SELECT count(*) FROM o_test_concurrent WHERE val > 500;

-- Cleanup
DROP TABLE o_test_concurrent CASCADE;
DROP SCHEMA concurrent_index CASCADE;
