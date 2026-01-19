setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_bank (
		id int PRIMARY KEY,
		balance int
	) USING orioledb;

	INSERT INTO o_bank VALUES (1, 0);
}

teardown
{
	DROP TABLE o_bank;
}

session "s1"
step "begin_1" { BEGIN; }
step "update_1" { UPDATE o_bank SET balance = balance + 1 WHERE id = 1; }
step "commit_1" { COMMIT; }
step "check_sum" { SELECT sum(balance) FROM o_bank; }

session "s2"
step "reindex" { REINDEX INDEX CONCURRENTLY o_bank_pkey; }

session "s3"
step "begin_3" { BEGIN; }
step "update_3" { UPDATE o_bank SET balance = balance + 1 WHERE id = 1; }
step "commit_3" { COMMIT; }

permutation "begin_1" "update_1" "reindex" "begin_3" "update_3" "commit_3" "commit_1" "check_sum"
