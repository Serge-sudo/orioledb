-- Test for ALTER INDEX RENAME functionality

CREATE EXTENSION IF NOT EXISTS orioledb;

CREATE TABLE test_rename_table (
    id int PRIMARY KEY,
    value text
) USING orioledb;

CREATE INDEX test_rename_idx ON test_rename_table(value);

-- Check initial index name
SELECT orioledb_tbl_indices('test_rename_table'::regclass);

-- Rename the index
ALTER INDEX test_rename_idx RENAME TO test_rename_idx_new;

-- Check the index name after rename
SELECT orioledb_tbl_indices('test_rename_table'::regclass);

-- Verify we can query using the new name
\d test_rename_idx_new

-- Clean up
DROP TABLE test_rename_table;
