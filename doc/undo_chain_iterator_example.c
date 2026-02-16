/*-------------------------------------------------------------------------
 *
 * undo_chain_iterator_example.c
 *		Example usage of the undo chain iterator
 *
 * This file demonstrates how to use btree_iterate_undo_chain() to traverse
 * all versions of tuples in a B-tree, including all items in their undo chains.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/doc/undo_chain_iterator_example.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * Example 1: Simple iteration over all tuples and their versions
 *
 * This example shows how to iterate through all tuples in a B-tree
 * and all their versions from undo chains.
 */
void
example1_iterate_all_versions(BTreeDescr *desc)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	int			total_versions = 0;
	int			current_tuple_num = 0;
	BTreeLeafTuphdr *prev_tupHdr = NULL;

	/* Create an iterator starting from the beginning */
	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;

		/* Get next version (current or undo) */
		tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
									   &scanEnd, NULL, &tupHdr);

		if (!O_TUPLE_IS_NULL(tup) || tupHdr != NULL)
		{
			/* Check if this is a new tuple (not an undo version) */
			if (prev_tupHdr == NULL ||
				prev_tupHdr->undoLocation != tupHdr->undoLocation)
			{
				if (prev_tupHdr != NULL)
					current_tuple_num++;
				elog(DEBUG1, "Tuple %d:", current_tuple_num);
			}

			total_versions++;

			/* Process version */
			elog(DEBUG2, "  Version %d: xactInfo=%lu, deleted=%d",
				 total_versions,
				 (unsigned long) tupHdr->xactInfo,
				 (int) tupHdr->deleted);

			prev_tupHdr = tupHdr;
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Scanned %d total versions", total_versions);
}

/*
 * Example 2: Counting versions per tuple
 *
 * This example shows how to count versions for each tuple.
 */
void
example2_count_versions_per_tuple(BTreeDescr *desc)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	int			total_tuples = 0;
	int			versions_for_current = 0;
	bool		in_undo_chain = false;

	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;

		tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
									   &scanEnd, NULL, &tupHdr);

		if (!O_TUPLE_IS_NULL(tup) || tupHdr != NULL)
		{
			/*
			 * Track if we're in an undo chain by checking if we were
			 * processing one and still are
			 */
			if (!in_undo_chain)
			{
				/* Starting a new tuple */
				if (versions_for_current > 0)
				{
					elog(LOG, "Tuple %d has %d versions",
						 total_tuples, versions_for_current);
				}
				total_tuples++;
				versions_for_current = 1;

				/* Check if this tuple has undo chain */
				if (UndoLocationIsValid(tupHdr->undoLocation))
					in_undo_chain = true;
			}
			else
			{
				/* Still in undo chain */
				versions_for_current++;

				/* Check if we're done with this chain */
				if (!UndoLocationIsValid(tupHdr->undoLocation))
					in_undo_chain = false;
			}
		}
	}

	/* Report last tuple */
	if (versions_for_current > 0)
	{
		elog(LOG, "Tuple %d has %d versions",
			 total_tuples, versions_for_current);
	}

	btree_iterator_free(it);

	elog(LOG, "Total tuples: %d", total_tuples);
}

/*
 * Example 3: Bounded iteration with end key
 *
 * This example shows how to iterate over a specific range of tuples
 * and their undo chains.
 */
void
example3_bounded_iteration(BTreeDescr *desc, void *startKey, void *endKey)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	int			version_count = 0;

	/* Create iterator starting from startKey */
	it = o_btree_iterator_create(desc, startKey,
								  startKey ? BTreeKeyBound : BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;

		/* Iterate with end boundary */
		tup = btree_iterate_undo_chain(it, endKey,
									   endKey ? BTreeKeyBound : BTreeKeyNone,
									   false, &scanEnd, NULL, &tupHdr);

		if (!O_TUPLE_IS_NULL(tup) || tupHdr != NULL)
		{
			version_count++;

			/* Process tuple version */
			if (tupHdr->deleted != BTreeLeafTupleNonDeleted)
			{
				elog(DEBUG1, "Version %d: DELETED (status=%d)",
					 version_count, (int) tupHdr->deleted);
			}
			else if (!O_TUPLE_IS_NULL(tup))
			{
				elog(DEBUG1, "Version %d: UPDATE", version_count);
			}
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Processed %d versions in range", version_count);
}

/*
 * Example 4: Collecting statistics about tuple versions
 *
 * This example shows how to collect detailed statistics during iteration.
 */
typedef struct
{
	int			total_tuples;
	int			total_versions;
	int			deleted_versions;
	int			update_versions;
	int			max_chain_length;
	int			current_chain_length;
} VersionStats;

void
example4_collect_statistics(BTreeDescr *desc)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	VersionStats stats = {0};
	bool		in_undo_chain = false;

	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;

		tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
									   &scanEnd, NULL, &tupHdr);

		if (!O_TUPLE_IS_NULL(tup) || tupHdr != NULL)
		{
			stats.total_versions++;

			if (!in_undo_chain)
			{
				/* New tuple */
				stats.total_tuples++;
				stats.current_chain_length = 1;

				if (UndoLocationIsValid(tupHdr->undoLocation))
					in_undo_chain = true;
			}
			else
			{
				/* Undo version */
				stats.current_chain_length++;

				if (!UndoLocationIsValid(tupHdr->undoLocation))
				{
					/* End of chain */
					if (stats.current_chain_length > stats.max_chain_length)
						stats.max_chain_length = stats.current_chain_length;
					in_undo_chain = false;
				}
			}

			/* Count version types */
			if (tupHdr->deleted != BTreeLeafTupleNonDeleted)
				stats.deleted_versions++;
			else if (!O_TUPLE_IS_NULL(tup))
				stats.update_versions++;
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Statistics:");
	elog(LOG, "  Total tuples: %d", stats.total_tuples);
	elog(LOG, "  Total versions: %d", stats.total_versions);
	elog(LOG, "  Deleted versions: %d", stats.deleted_versions);
	elog(LOG, "  Update versions: %d", stats.update_versions);
	elog(LOG, "  Max chain length: %d", stats.max_chain_length);
	elog(LOG, "  Avg versions per tuple: %.2f",
		 (float) stats.total_versions / stats.total_tuples);
}

/*
 * Example 5: Finding tuples with long undo chains
 *
 * This example shows how to identify tuples with many versions.
 */
void
example5_find_long_chains(BTreeDescr *desc, int min_chain_length)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	int			current_chain_length = 0;
	int			tuple_num = 0;
	bool		in_undo_chain = false;

	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;

		tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
									   &scanEnd, NULL, &tupHdr);

		if (!O_TUPLE_IS_NULL(tup) || tupHdr != NULL)
		{
			if (!in_undo_chain)
			{
				/* Report previous tuple if it had a long chain */
				if (current_chain_length >= min_chain_length)
				{
					elog(LOG, "Tuple %d has long chain: %d versions",
						 tuple_num, current_chain_length);
				}

				/* Start new tuple */
				tuple_num++;
				current_chain_length = 1;

				if (UndoLocationIsValid(tupHdr->undoLocation))
					in_undo_chain = true;
			}
			else
			{
				current_chain_length++;

				if (!UndoLocationIsValid(tupHdr->undoLocation))
					in_undo_chain = false;
			}
		}
	}

	/* Check last tuple */
	if (current_chain_length >= min_chain_length)
	{
		elog(LOG, "Tuple %d has long chain: %d versions",
			 tuple_num, current_chain_length);
	}

	btree_iterator_free(it);
}
