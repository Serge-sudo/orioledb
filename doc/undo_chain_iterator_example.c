/*-------------------------------------------------------------------------
 *
 * undo_chain_iterator_example.c
 *		Example usage of the undo chain iterator functions
 *
 * This file demonstrates how to use btree_iterate_undo_chain() and
 * o_walk_undo_chain() to traverse all versions of tuples in a B-tree,
 * including all items in their undo chains.
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
 * and process each version in their undo chains.
 */
void
example1_iterate_all_versions(BTreeDescr *desc)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	int			total_tuples = 0;
	int			total_versions = 0;

	/* Create an iterator starting from the beginning */
	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		UndoLocation undoLoc;
		OTuple		tup;

		/* Get next tuple from leaf pages */
		tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
									   &scanEnd, NULL, &tupHdr, &undoLoc);

		if (!O_TUPLE_IS_NULL(tup))
		{
			total_tuples++;
			total_versions++;	/* Count current version */

			/* Process current version */
			elog(DEBUG1, "Tuple %d: xactInfo=%lu, deleted=%d",
				 total_tuples,
				 (unsigned long) tupHdr->xactInfo,
				 (int) tupHdr->deleted);

			/* Walk undo chain if available */
			if (UndoLocationIsValid(undoLoc))
			{
				BTreeLeafTuphdr undoHdr = *tupHdr;
				int			version_num = 1;

				/* Simple inline processing without callback */
				while (UndoLocationIsValid(undoHdr.undoLocation))
				{
					UndoLocation prevUndoLoc = undoHdr.undoLocation;
					OTuple		undoTup;

					if (undoHdr.deleted != BTreeLeafTupleNonDeleted ||
						XACT_INFO_IS_LOCK_ONLY(undoHdr.xactInfo))
					{
						get_prev_leaf_header_from_undo(desc->undoType,
													   &undoHdr, true);
					}
					else
					{
						get_prev_leaf_header_and_tuple_from_undo(desc->undoType,
																 &undoHdr,
																 &undoTup, 0);
						pfree(undoTup.data);
					}

					total_versions++;
					version_num++;

					elog(DEBUG2, "  Version %d: xactInfo=%lu, deleted=%d",
						 version_num,
						 (unsigned long) undoHdr.xactInfo,
						 (int) undoHdr.deleted);
				}
			}
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Scanned %d tuples with %d total versions",
		 total_tuples, total_versions);
}

/*
 * Example 2: Using callback to process undo chain versions
 *
 * This example shows how to use o_walk_undo_chain with a callback
 * function to process each version.
 */

/* Context structure for callback */
typedef struct
{
	int			version_count;
	int			deleted_count;
	int			update_count;
	BTreeDescr *desc;
} VersionStats;

/* Callback function for processing each version */
static bool
count_versions_callback(OTuple tuple, BTreeLeafTuphdr *tupHdr, void *arg)
{
	VersionStats *stats = (VersionStats *) arg;

	stats->version_count++;

	if (tupHdr->deleted != BTreeLeafTupleNonDeleted)
	{
		stats->deleted_count++;
		elog(DEBUG2, "  Version %d: DELETED (deleted=%d)",
			 stats->version_count, (int) tupHdr->deleted);
	}
	else if (XACT_INFO_IS_LOCK_ONLY(tupHdr->xactInfo))
	{
		elog(DEBUG2, "  Version %d: LOCK-ONLY", stats->version_count);
	}
	else if (!O_TUPLE_IS_NULL(tuple))
	{
		stats->update_count++;
		elog(DEBUG2, "  Version %d: UPDATE", stats->version_count);
	}

	/* Continue iteration */
	return true;
}

void
example2_count_versions_with_callback(BTreeDescr *desc)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	VersionStats total_stats = {0};

	total_stats.desc = desc;

	it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		UndoLocation undoLoc;
		OTuple		tup;
		VersionStats tuple_stats = {0};

		tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
									   &scanEnd, NULL, &tupHdr, &undoLoc);

		if (!O_TUPLE_IS_NULL(tup))
		{
			tuple_stats.desc = desc;

			elog(DEBUG1, "Processing tuple with %d deleted status",
				 (int) tupHdr->deleted);

			/* Walk undo chain with callback */
			if (UndoLocationIsValid(undoLoc))
			{
				BTreeLeafTuphdr undoHdr = *tupHdr;
				o_walk_undo_chain(desc, &undoHdr, CurrentMemoryContext,
								  count_versions_callback, &tuple_stats);

				elog(DEBUG1, "Tuple has %d versions (%d updates, %d deletes)",
					 tuple_stats.version_count,
					 tuple_stats.update_count,
					 tuple_stats.deleted_count);

				/* Accumulate stats */
				total_stats.version_count += tuple_stats.version_count;
				total_stats.deleted_count += tuple_stats.deleted_count;
				total_stats.update_count += tuple_stats.update_count;
			}
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Total: %d versions (%d updates, %d deletes)",
		 total_stats.version_count,
		 total_stats.update_count,
		 total_stats.deleted_count);
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
	int			tuple_count = 0;

	/* Create iterator starting from startKey */
	it = o_btree_iterator_create(desc, startKey,
								  startKey ? BTreeKeyBound : BTreeKeyNone,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		UndoLocation undoLoc;
		OTuple		tup;

		/* Iterate with end boundary */
		tup = btree_iterate_undo_chain(it, endKey,
									   endKey ? BTreeKeyBound : BTreeKeyNone,
									   false, &scanEnd, NULL, &tupHdr, &undoLoc);

		if (!O_TUPLE_IS_NULL(tup))
		{
			tuple_count++;

			/* Process tuple and its undo chain */
			if (UndoLocationIsValid(undoLoc))
			{
				BTreeLeafTuphdr undoHdr = *tupHdr;
				int			version_count = 0;

				while (UndoLocationIsValid(undoHdr.undoLocation))
				{
					version_count++;

					if (undoHdr.deleted != BTreeLeafTupleNonDeleted ||
						XACT_INFO_IS_LOCK_ONLY(undoHdr.xactInfo))
					{
						get_prev_leaf_header_from_undo(desc->undoType,
													   &undoHdr, true);
					}
					else
					{
						OTuple		undoTup;

						get_prev_leaf_header_and_tuple_from_undo(desc->undoType,
																 &undoHdr,
																 &undoTup, 0);
						pfree(undoTup.data);
					}
				}

				elog(DEBUG1, "Tuple %d has %d versions in undo chain",
					 tuple_count, version_count);
			}
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Processed %d tuples in range", tuple_count);
}

/*
 * Example 4: Collecting all versions of a specific tuple
 *
 * This example shows how to collect all versions of a tuple
 * for analysis or debugging purposes.
 */

typedef struct
{
	List	   *versions;	/* List of collected tuple versions */
	int			max_versions;	/* Maximum versions to collect */
	MemoryContext mcxt;		/* Memory context for allocations */
	BTreeDescr *desc;		/* B-tree descriptor for tuple operations */
} CollectVersionsContext;

typedef struct
{
	OTuple		tuple;
	BTreeLeafTuphdr tupHdr;
} TupleVersion;

static bool
collect_versions_callback(OTuple tuple, BTreeLeafTuphdr *tupHdr, void *arg)
{
	CollectVersionsContext *ctx = (CollectVersionsContext *) arg;
	TupleVersion *version;

	/* Check if we've hit the limit */
	if (ctx->max_versions > 0 &&
		list_length(ctx->versions) >= ctx->max_versions)
	{
		return false;			/* Stop iteration */
	}

	/* Allocate and store this version */
	version = (TupleVersion *) MemoryContextAlloc(ctx->mcxt,
												  sizeof(TupleVersion));
	version->tupHdr = *tupHdr;

	if (!O_TUPLE_IS_NULL(tuple))
	{
		BTreeDescr *desc = ctx->desc;
		int			tuple_len;

		/* Get the actual tuple length using the descriptor */
		tuple_len = o_btree_len(desc, tuple, OTupleLength);

		version->tuple.data = (Pointer) MemoryContextAlloc(ctx->mcxt, tuple_len);
		memcpy(version->tuple.data, tuple.data, tuple_len);
		version->tuple.formatFlags = tuple.formatFlags;
	}
	else
	{
		O_TUPLE_SET_NULL(version->tuple);
	}

	ctx->versions = lappend(ctx->versions, version);

	return true;				/* Continue iteration */
}

List *
example4_collect_tuple_versions(BTreeDescr *desc, void *key, int max_versions,
								MemoryContext mcxt)
{
	BTreeIterator *it;
	bool		scanEnd = false;
	CollectVersionsContext ctx;

	ctx.versions = NIL;
	ctx.max_versions = max_versions;
	ctx.mcxt = mcxt;
	ctx.desc = desc;

	/* Create iterator for specific key */
	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								  NULL, ForwardScanDirection);

	while (!scanEnd)
	{
		BTreeLeafTuphdr *tupHdr;
		UndoLocation undoLoc;
		OTuple		tup;

		tup = btree_iterate_undo_chain(it, key, BTreeKeyBound,
									   true, &scanEnd, NULL, &tupHdr, &undoLoc);

		if (!O_TUPLE_IS_NULL(tup))
		{
			/* Found our tuple, collect all its versions */
			if (UndoLocationIsValid(undoLoc))
			{
				BTreeLeafTuphdr undoHdr = *tupHdr;
				o_walk_undo_chain(desc, &undoHdr, mcxt,
								  collect_versions_callback, &ctx);
			}

			/* We only want versions of this specific tuple */
			break;
		}
	}

	btree_iterator_free(it);

	elog(LOG, "Collected %d versions of tuple", list_length(ctx.versions));

	return ctx.versions;
}
