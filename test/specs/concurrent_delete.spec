# Test concurrent delete operations on the same tuple
# This test demonstrates what happens when two transactions try to simultaneously
# delete the same tuple, focusing on wait mechanisms and undo log behavior

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_concurrent_delete (
		id int PRIMARY KEY,
		value text
	) USING orioledb;

	INSERT INTO o_concurrent_delete VALUES (1, 'initial');
	INSERT INTO o_concurrent_delete VALUES (2, 'second');
	INSERT INTO o_concurrent_delete VALUES (3, 'third');
}

teardown
{
	DROP TABLE o_concurrent_delete;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_delete_1" { DELETE FROM o_concurrent_delete WHERE id = 1; }
step "s1_delete_2" { DELETE FROM o_concurrent_delete WHERE id = 2; }
step "s1_select" { SELECT * FROM o_concurrent_delete ORDER BY id; }
step "s1_commit" { COMMIT; }
step "s1_rollback" { ROLLBACK; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_delete_1" { DELETE FROM o_concurrent_delete WHERE id = 1; }
step "s2_delete_2" { DELETE FROM o_concurrent_delete WHERE id = 2; }
step "s2_select" { SELECT * FROM o_concurrent_delete ORDER BY id; }
step "s2_commit" { COMMIT; }
step "s2_rollback" { ROLLBACK; }

session "s3"
step "s3_select" { SELECT * FROM o_concurrent_delete ORDER BY id; }

# Permutation 1: T1 deletes and commits, then T2 tries to delete the same tuple
# Expected: T2 waits for T1, then finds tuple already deleted (0 rows affected)
permutation "s1_begin" "s2_begin"
           "s1_delete_1"
           "s2_delete_1"     # T2 waits for T1
           "s1_commit"       # T1 commits, T2 wakes up
           "s2_commit"       # T2 completes with 0 rows
           "s3_select"       # Verify tuple is deleted

# Permutation 2: T1 deletes and rolls back, then T2 successfully deletes
# Expected: T2 waits for T1, T1 rolls back via undo log, T2 proceeds to delete
permutation "s1_begin" "s2_begin"
           "s1_delete_1"
           "s2_delete_1"     # T2 waits for T1
           "s1_rollback"     # T1 aborts, undo log restores tuple
           "s2_commit"       # T2 successfully deletes (1 row affected)
           "s3_select"       # Verify tuple is deleted

# Permutation 3: Three transactions all trying to delete the same tuple
# Expected: Serialization based on lock acquisition order
permutation "s1_begin" "s2_begin"
           "s1_delete_1"
           "s2_delete_1"     # T2 waits for T1
           "s1_rollback"     # T1 rolls back
           "s2_commit"       # T2 deletes the tuple
           "s3_select"       # Verify final state

# Permutation 4: Concurrent deletes of different tuples (no conflict)
# Expected: Both transactions proceed without waiting
permutation "s1_begin" "s2_begin"
           "s1_delete_1"     # T1 deletes id=1
           "s2_delete_2"     # T2 deletes id=2 (no conflict)
           "s1_commit"
           "s2_commit"
           "s3_select"       # Both tuples deleted

# Permutation 5: Same transaction deletes twice (should be no-op on second delete)
permutation "s1_begin"
           "s1_delete_1"
           "s1_select"       # Verify delete in progress
           "s1_delete_1"     # Delete again - 0 rows affected
           "s1_commit"
           "s3_select"

# Permutation 6: Verify visibility during concurrent delete
permutation "s1_begin" "s2_begin"
           "s1_delete_1"
           "s1_select"       # T1 sees tuple deleted
           "s2_select"       # T2 still sees tuple (MVCC)
           "s2_delete_1"     # T2 waits
           "s1_commit"       # T1 commits
           "s2_select"       # T2 now sees tuple deleted
           "s2_commit"
           "s3_select"
