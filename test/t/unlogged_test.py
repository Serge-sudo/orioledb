#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from testgres.exceptions import QueryException


class UnloggedTest(BaseTest):

	def test_unlogged_table_checkpoint_recovery(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE UNLOGGED TABLE o_test_1 (
				val_1 serial PRIMARY KEY,
				val_2 int,
				val_3 int,
				val_4 int
			) USING orioledb;

			INSERT INTO o_test_1 (val_1, val_2, val_3) VALUES (1,2,3);

			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1;"), [])

		node.stop()

	def test_unlogged_table_recovery_checkpoint(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE UNLOGGED TABLE o_test_1 (
				val_1 int
			) USING orioledb;

			INSERT INTO o_test_1 VALUES (1);
		""")

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT * FROM o_test_1;"), [])
		node.safe_psql("""
			INSERT INTO o_test_1 VALUES (1);
		""")
		node.stop()

		node.start()
		self.assertEqual(node.execute("SELECT * FROM o_test_1;"), [(1, )])
		node.stop()

	def test_unlogged_table_eviction_after_checkpoint(self):
		"""
		Test that unlogged table root eviction works correctly after a
		checkpoint.  Previously, checkpoint_init_new_seq_bufs() did not
		initialize the nextChkp seq buf for unlogged tables, causing an
		assertion failure in btree_finalize_private_seq_bufs() when the
		bgwriter tried to evict the root page.
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', """
					orioledb.main_buffers = 8MB
					orioledb.debug_disable_bgwriter = true
				""")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE UNLOGGED TABLE o_unlogged (
				key SERIAL PRIMARY KEY,
				val int NOT NULL
			) USING orioledb;

			INSERT INTO o_unlogged (val)
				SELECT val FROM generate_series(1, 5000, 1) val;

			CHECKPOINT;
		""")

		# Insert into a large regular table to fill the buffer pool and
		# force eviction of the unlogged table's root page.
		node.safe_psql("""
			CREATE TABLE o_big (
				key SERIAL PRIMARY KEY,
				val int NOT NULL
			) USING orioledb;

			INSERT INTO o_big (val)
				SELECT val FROM generate_series(1, 100000, 1) val;
		""")

		# A second checkpoint exercises the reload-from-eviction path.
		node.safe_psql("CHECKPOINT;")

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_unlogged;")[0][0], 5000)

		node.stop()

	def test_unlogged_table_replication(self):
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE UNLOGGED TABLE o_test_1(
							val_1 int
						) USING orioledb;

						INSERT INTO o_test_1 VALUES (1);
					""")

					con1.commit()

					self.catchup_orioledb(replica)

					self.assertEqual(master.execute("SELECT * FROM o_test_1;"),
					                 [(1, )])
					with self.assertRaises(QueryException) as e:
						replica.safe_psql("SELECT * FROM o_test_1;")
					self.assertErrorMessageEquals(e, (
					    f"cannot access temporary or unlogged relations during recovery"
					))
