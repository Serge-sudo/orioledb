-- Test for bounded local page pool with clock sweep eviction.
-- Uses a small pool size to force eviction of pages to the spill file.

CREATE SCHEMA temp_bounded_schema;
SET SESSION search_path = 'temp_bounded_schema';
CREATE EXTENSION orioledb;
SET orioledb.enable_local_page_pool = on;

-- Set a small bounded pool size to force eviction
SET orioledb.local_page_pool_size = '80kB';

-- Basic test: create a temp table and insert rows that exceed the pool size
CREATE TEMP TABLE o_test_bounded (
	id int,
	val text
) USING orioledb;

-- Insert enough rows to force eviction of some leaf pages
INSERT INTO o_test_bounded
    SELECT val, 'value_' || val FROM generate_series(1, 50) val;

-- Verify data is correct after eviction/reloading
SELECT count(*) FROM o_test_bounded;
SELECT * FROM o_test_bounded ORDER BY id LIMIT 5;
SELECT * FROM o_test_bounded ORDER BY id DESC LIMIT 5;

-- Test with index to force more page allocation
CREATE INDEX o_test_bounded_val_idx ON o_test_bounded(val);

INSERT INTO o_test_bounded
    SELECT val, 'newval_' || val FROM generate_series(51, 100) val;

SELECT count(*) FROM o_test_bounded;
SELECT * FROM o_test_bounded WHERE id = 75;

-- Test with larger pool size (no eviction needed)
SET orioledb.local_page_pool_size = '8MB';

CREATE TEMP TABLE o_test_bounded_large (
	id int,
	val text
) USING orioledb;

INSERT INTO o_test_bounded_large
    SELECT val, 'value_' || val FROM generate_series(1, 100) val;

SELECT count(*) FROM o_test_bounded_large;

-- Verify GUC reset (-1 = unbounded mode)
SET orioledb.local_page_pool_size = -1;

CREATE TEMP TABLE o_test_unbounded (
	id int
) USING orioledb;

INSERT INTO o_test_unbounded
    SELECT val FROM generate_series(1, 100) val;

SELECT count(*) FROM o_test_unbounded;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA temp_bounded_schema CASCADE;
RESET orioledb.enable_local_page_pool;
RESET orioledb.local_page_pool_size;
RESET search_path;
