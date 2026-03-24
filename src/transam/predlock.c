/*-------------------------------------------------------------------------
 *
 * predlock.c
 *		OrioleDB predicate locking implementation.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/transam/predlock.c
 *
 * OVERVIEW
 *	  See include/transam/predlock.h for the design overview.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/modify.h"
#include "btree/page_contents.h"
#include "transam/oxid.h"
#include "transam/predlock.h"

#include "storage/lwlock.h"
#include "miscadmin.h"

/* Global pointer to the per-backend predicate lock tables in shared memory. */
OPredLocksData *o_pred_locks = NULL;

/* --------------------------------------------------------------------------
 * Shared memory helpers
 * --------------------------------------------------------------------------
 */

Size
o_pred_lock_shmem_size(void)
{
	return mul_size(max_procs, sizeof(OPredLocksData));
}

void
o_pred_lock_shmem_init(Pointer ptr, bool found)
{
	o_pred_locks = (OPredLocksData *) ptr;

	if (!found)
	{
		int			i;
		int			trancheId = LWLockNewTrancheId();

		LWLockRegisterTranche(trancheId, "orioledb_pred_locks");

		for (i = 0; i < max_procs; i++)
		{
			o_pred_locks[i].trancheId = trancheId;
			LWLockInitialize(&o_pred_locks[i].lock, trancheId);
			o_pred_locks[i].numValid = 0;
			memset(o_pred_locks[i].entries, 0,
				   sizeof(o_pred_locks[i].entries));
		}
	}
}

/* --------------------------------------------------------------------------
 * Internal helpers
 * --------------------------------------------------------------------------
 */

/*
 * Copy a key from an OTuple into an OPredLockKey.  Returns true on success,
 * false when the key is too long to store inline (caller should promote to a
 * coarser lock level).
 */
static bool
predlock_key_from_tuple(BTreeDescr *desc, OTuple tuple, OPredLockKey *dst)
{
	int			len;

	if (O_TUPLE_IS_NULL(tuple))
	{
		dst->notNull = false;
		dst->formatFlags = 0;
		dst->len = 0;
		return true;
	}

	len = o_btree_len(desc, tuple, OKeyLength);
	if (len > O_PRED_LOCK_MAX_KEY_SIZE)
		return false;

	dst->notNull = true;
	dst->formatFlags = tuple.formatFlags;
	dst->len = (uint16) len;
	memcpy(dst->data, tuple.data, len);
	return true;
}

/*
 * Copy a hikey from a B-tree page into an OPredLockKey.
 * If the page is the rightmost page, the hikey is "positive infinity"
 * (represented as notNull = false).
 */
static bool
predlock_hikey_from_page(BTreeDescr *desc, Page page, OPredLockKey *dst)
{
	if (O_PAGE_IS(page, RIGHTMOST))
	{
		/* Rightmost page – hikey is "infinity". */
		dst->notNull = false;
		dst->formatFlags = 0;
		dst->len = 0;
		return true;
	}
	else
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, page);
		return predlock_key_from_tuple(desc, hikey, dst);
	}
}

/*
 * Get an OTuple from an OPredLockKey (points into the key's own data array).
 */
static OTuple
predlock_key_to_tuple(OPredLockKey *key)
{
	OTuple		t;

	if (!key->notNull)
	{
		t.data = NULL;
		t.formatFlags = 0;
	}
	else
	{
		t.data = key->data;
		t.formatFlags = key->formatFlags;
	}
	return t;
}

/*
 * Compare a B-tree key against an OPredLockKey.
 *
 * Returns a value consistent with o_btree_cmp():
 *   < 0  if key < stored key
 *   = 0  if key == stored key
 *   > 0  if key > stored key
 *
 * A "not-null" stored key is treated as the corresponding B-tree key.
 * A "null" stored hikey represents +infinity (always > anything).
 * A "null" stored lokey represents -infinity (always < anything).
 */
static int
predlock_cmp_with_hikey(BTreeDescr *desc,
						OTuple key, BTreeKeyType keyType,
						OPredLockKey *stored)
{
	OTuple		storedTuple;

	if (!stored->notNull)
		return -1;				/* key < +infinity */

	storedTuple = predlock_key_to_tuple(stored);
	return o_btree_cmp(desc, &key, keyType,
					   &storedTuple, BTreeKeyNonLeafKey);
}

static int
predlock_cmp_with_lokey(BTreeDescr *desc,
						OTuple key, BTreeKeyType keyType,
						OPredLockKey *stored)
{
	OTuple		storedTuple;

	if (!stored->notNull)
		return 1;				/* key > -infinity */

	storedTuple = predlock_key_to_tuple(stored);
	return o_btree_cmp(desc, &key, keyType,
					   &storedTuple, BTreeKeyNonLeafKey);
}

/*
 * Compare two stored predicate lock keys.
 *
 * aIsHi/bIsHi determine whether NULL means +infinity (hi key) or -infinity
 * (lo key).
 */
static int
predlock_cmp_stored_keys(BTreeDescr *desc,
						 OPredLockKey *a, bool aIsHi,
						 OPredLockKey *b, bool bIsHi)
{
	if (!a->notNull && !b->notNull)
		return 0;
	if (!a->notNull)
		return aIsHi ? 1 : -1;
	if (!b->notNull)
		return bIsHi ? -1 : 1;

	{
		OTuple		ta = predlock_key_to_tuple(a);
		OTuple		tb = predlock_key_to_tuple(b);

		return o_btree_cmp(desc, &ta, BTreeKeyNonLeafKey,
						   &tb, BTreeKeyNonLeafKey);
	}
}

static bool
page_range_contains(BTreeDescr *desc,
					OPredLockKey *outerLo, OPredLockKey *outerHi,
					OPredLockKey *innerLo, OPredLockKey *innerHi)
{
	return predlock_cmp_stored_keys(desc, outerLo, false, innerLo, false) <= 0 &&
		predlock_cmp_stored_keys(desc, outerHi, true, innerHi, true) >= 0;
}

static bool
page_ranges_overlap_or_touch(BTreeDescr *desc,
							 OPredLockKey *lo1, OPredLockKey *hi1,
							 OPredLockKey *lo2, OPredLockKey *hi2)
{
	OPredLockKey *leftLo,
			   *leftHi,
			   *rightLo,
			   *rightHi;

	if (predlock_cmp_stored_keys(desc, lo1, false, lo2, false) <= 0)
	{
		leftLo = lo1;
		leftHi = hi1;
		rightLo = lo2;
		rightHi = hi2;
	}
	else
	{
		leftLo = lo2;
		leftHi = hi2;
		rightLo = lo1;
		rightHi = hi1;
	}

	return predlock_cmp_stored_keys(desc, leftHi, true, rightLo, false) >= 0;
}

static void
page_range_widen(BTreeDescr *desc,
				 OPredLockKey *dstLo, OPredLockKey *dstHi,
				 OPredLockKey *srcLo, OPredLockKey *srcHi)
{
	if (predlock_cmp_stored_keys(desc, dstLo, false, srcLo, false) > 0)
		*dstLo = *srcLo;
	if (predlock_cmp_stored_keys(desc, dstHi, true, srcHi, true) < 0)
		*dstHi = *srcHi;
}

static BTreeDescr *page_lock_cmp_desc = NULL;
static OPredLocksData *page_lock_cmp_tbl = NULL;

static int
page_lock_index_cmp(const void *pa, const void *pb)
{
	int			ia = *(const int *) pa;
	int			ib = *(const int *) pb;
	OPredLockEntry *a = &page_lock_cmp_tbl->entries[ia];
	OPredLockEntry *b = &page_lock_cmp_tbl->entries[ib];
	int			cmp;

	cmp = predlock_cmp_stored_keys(page_lock_cmp_desc,
								   &a->loKey, false,
								   &b->loKey, false);
	if (cmp != 0)
		return cmp;

	return predlock_cmp_stored_keys(page_lock_cmp_desc,
									&a->key, true,
									&b->key, true);
}

/*
 * Collapse overlapping or adjacent page-level intervals for a given tree/oxid.
 */
static void
coalesce_page_intervals(BTreeDescr *desc, OPredLocksData *tbl,
						ORelOids oids, OXid oxid)
{
	int			idx[O_PRED_LOCKS_MAX_ENTRIES];
	int			n = 0;
	int			i;

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *e = &tbl->entries[i];

		if (!e->valid || e->level != OPredLockLevelPage)
			continue;
		if (!ORelOidsIsEqual(e->oids, oids) || e->oxid != oxid)
			continue;
		idx[n++] = i;
	}

	if (n < 2)
		return;

	page_lock_cmp_desc = desc;
	page_lock_cmp_tbl = tbl;
	qsort(idx, n, sizeof(int), page_lock_index_cmp);

	{
		int			anchorIdx = idx[0];
		OPredLockEntry *anchor = &tbl->entries[anchorIdx];

		for (i = 1; i < n; i++)
		{
			int			curIdx = idx[i];
			OPredLockEntry *cur = &tbl->entries[curIdx];

			if (!cur->valid)
				continue;

			if (page_ranges_overlap_or_touch(desc,
											 &anchor->loKey, &anchor->key,
											 &cur->loKey, &cur->key))
			{
				page_range_widen(desc, &anchor->loKey, &anchor->key,
								 &cur->loKey, &cur->key);
				cur->valid = false;
				tbl->numValid--;
			}
			else
			{
				anchorIdx = curIdx;
				anchor = &tbl->entries[anchorIdx];
			}
		}
	}
}

/*
 * Merge the pair of intervals with the smallest gap (overlap/adjacent preferred)
 * for the given tree/oxid.  Returns true if a merge happened.
 */
static bool
merge_closest_page_intervals(BTreeDescr *desc, OPredLocksData *tbl,
							 ORelOids oids, OXid oxid)
{
	int			idx[O_PRED_LOCKS_MAX_ENTRIES];
	int			n = 0;
	int			i;
	int			bestLeft = -1,
				bestRight = -1;
	int			bestGap = INT_MAX;

	coalesce_page_intervals(desc, tbl, oids, oxid);

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *e = &tbl->entries[i];

		if (!e->valid || e->level != OPredLockLevelPage)
			continue;
		if (!ORelOidsIsEqual(e->oids, oids) || e->oxid != oxid)
			continue;
		idx[n++] = i;
	}

	if (n < 2)
		return false;

	page_lock_cmp_desc = desc;
	page_lock_cmp_tbl = tbl;
	qsort(idx, n, sizeof(int), page_lock_index_cmp);

	for (i = 0; i < n - 1; i++)
	{
		OPredLockEntry *left = &tbl->entries[idx[i]];
		OPredLockEntry *right = &tbl->entries[idx[i + 1]];
		int			gapScore;

		if (page_ranges_overlap_or_touch(desc,
										 &left->loKey, &left->key,
										 &right->loKey, &right->key))
			gapScore = 0;
		else
			gapScore = 1;

		if (gapScore < bestGap)
		{
			bestGap = gapScore;
			bestLeft = idx[i];
			bestRight = idx[i + 1];

			if (gapScore == 0)
				break;
		}
	}

	if (bestLeft < 0 || bestRight < 0)
		return false;

	page_range_widen(desc,
					 &tbl->entries[bestLeft].loKey,
					 &tbl->entries[bestLeft].key,
					 &tbl->entries[bestRight].loKey,
					 &tbl->entries[bestRight].key);
	tbl->entries[bestRight].valid = false;
	tbl->numValid--;
	return true;
}

/*
 * Does the key (keyType) fall within a page-level lock entry whose range is
 * [loKey, hiKey)?  The hiKey is exclusive (key < hiKey), loKey is inclusive
 * (key >= loKey).
 */
static bool
key_in_page_lock(BTreeDescr *desc,
				 OTuple key, BTreeKeyType keyType,
				 OPredLockEntry *entry)
{
	Assert(entry->level == OPredLockLevelPage);

	/* key must be >= loKey */
	if (predlock_cmp_with_lokey(desc, key, keyType, &entry->loKey) < 0)
		return false;

	/* key must be < hiKey  (hiKey is the *exclusive* upper bound) */
	if (predlock_cmp_with_hikey(desc, key, keyType, &entry->key) >= 0)
		return false;

	return true;
}

/*
 * Does the entry cover the given key?  Depends on the lock level.
 */
static bool
entry_covers_key(BTreeDescr *desc,
				 OTuple key, BTreeKeyType keyType,
				 OPredLockEntry *entry)
{
	switch (entry->level)
	{
		case OPredLockLevelTuple:
			{
				OTuple		stored = predlock_key_to_tuple(&entry->key);

				if (!stored.data)
					return false;
				return (o_btree_cmp(desc, &key, keyType,
									&stored, BTreeKeyNonLeafKey) == 0);
			}

		case OPredLockLevelPage:
			return key_in_page_lock(desc, key, keyType, entry);

		case OPredLockLevelTree:
			return true;
	}
	return false;				/* unreachable */
}

/*
 * Count entries with the given oids and (for page-level matching) the same
 * hikey.  This is used to decide whether to promote.
 */
static int
count_tuple_entries_on_page(OPredLocksData *tbl,
							ORelOids oids, OXid oxid,
							OPredLockKey *pageHikey)
{
	int			count = 0;
	int			i;

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *e = &tbl->entries[i];

		if (!e->valid || e->level != OPredLockLevelTuple)
			continue;
		if (!ORelOidsIsEqual(e->oids, oids))
			continue;
		if (e->oxid != oxid)
			continue;
		/* Same page? Compare hikeys. */
		if (e->key.notNull != pageHikey->notNull)
			continue;
		if (e->key.notNull &&
			(e->key.len != pageHikey->len ||
			 e->key.formatFlags != pageHikey->formatFlags ||
			 memcmp(e->key.data, pageHikey->data, e->key.len) != 0))
			continue;
		count++;
	}
	return count;
}

/*
 * Remove all tuple-level entries that belong to the given page (identified
 * by its hikey and lokey).  Used when promoting tuple locks to a page lock.
 */
static void
remove_tuple_entries_on_page(OPredLocksData *tbl,
							 ORelOids oids, OXid oxid,
							 OPredLockKey *pageHikey)
{
	int			i;

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *e = &tbl->entries[i];

		if (!e->valid || e->level != OPredLockLevelTuple)
			continue;
		if (!ORelOidsIsEqual(e->oids, oids))
			continue;
		if (e->oxid != oxid)
			continue;
		if (e->key.notNull != pageHikey->notNull)
			continue;
		if (e->key.notNull &&
			(e->key.len != pageHikey->len ||
			 e->key.formatFlags != pageHikey->formatFlags ||
			 memcmp(e->key.data, pageHikey->data, e->key.len) != 0))
			continue;
		e->valid = false;
		tbl->numValid--;
	}
}

/*
 * Remove all page-level entries for a given tree.  Used when promoting page
 * locks to a tree lock.
 */
static void
remove_page_entries_for_tree(OPredLocksData *tbl,
							 ORelOids oids, OXid oxid)
{
	int			i;

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *e = &tbl->entries[i];

		if (!e->valid || e->level != OPredLockLevelPage)
			continue;
		if (!ORelOidsIsEqual(e->oids, oids))
			continue;
		if (e->oxid != oxid)
			continue;
		e->valid = false;
		tbl->numValid--;
	}
}

/*
 * Find a free slot in tbl->entries[].  Returns the index, or -1 if the
 * table is full.
 */
static int
find_free_slot(OPredLocksData *tbl)
{
	int			i;

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		if (!tbl->entries[i].valid)
			return i;
	}
	return -1;
}

/*
 * Check whether tbl already contains a lock entry for the same key that is
 * at least as strong as what we want to add (same oids, same or coarser
 * level, same oxid).  Returns true when the new lock would be redundant.
 */
static bool
already_locked(BTreeDescr *desc,
			   OPredLocksData *tbl,
			   ORelOids oids, OXid oxid,
			   OTuple key, OPredLockKey *pageHikey, OPredLockKey *pageLokey)
{
	int			i;

	for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *e = &tbl->entries[i];

		if (!e->valid)
			continue;
		if (e->oxid != oxid)
			continue;
		if (!ORelOidsIsEqual(e->oids, oids))
			continue;

		switch (e->level)
		{
			case OPredLockLevelTree:
				return true;

			case OPredLockLevelPage:
				/* Does this page lock cover the key? */
				if (key_in_page_lock(desc, key, BTreeKeyNonLeafKey, e))
					return true;
				break;

			case OPredLockLevelTuple:
				{
					OTuple		stored = predlock_key_to_tuple(&e->key);

					if (stored.data &&
						o_btree_cmp(desc, &key, BTreeKeyNonLeafKey,
									&stored, BTreeKeyNonLeafKey) == 0)
						return true;
					break;
				}
		}
	}
	return false;
}

/*
 * Insert a page-level lock entry into tbl.  Page locks are kept as disjoint,
 * monotonically increasing intervals; overlapping or adjacent locks are merged.
 * If we run out of space we first merge the closest page intervals before
 * falling back to a tree-level lock.
 */
static void
insert_page_lock(BTreeDescr *desc,
				 OPredLocksData *tbl,
				 ORelOids oids, OXid oxid,
				 OPredLockKey *pageHikey, OPredLockKey *pageLokey)
{
	int			slot;
	OPredLockEntry *e;
	OPredLockKey mergedHi = *pageHikey;
	OPredLockKey mergedLo = *pageLokey;

	/* Tree-level lock already covers everything. */
	for (int i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *ex = &tbl->entries[i];

		if (!ex->valid || ex->level != OPredLockLevelTree)
			continue;
		if (ORelOidsIsEqual(ex->oids, oids) && ex->oxid == oxid)
			return;
	}

	/* Merge with existing page locks of the same tree/transaction. */
	for (int i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *ex = &tbl->entries[i];

		if (!ex->valid || ex->level != OPredLockLevelPage)
			continue;
		if (!ORelOidsIsEqual(ex->oids, oids) || ex->oxid != oxid)
			continue;

		if (page_range_contains(desc,
								&ex->loKey, &ex->key,
								&mergedLo, &mergedHi))
			return;				/* already covered */

		if (page_range_contains(desc,
								&mergedLo, &mergedHi,
								&ex->loKey, &ex->key) ||
			page_ranges_overlap_or_touch(desc,
										 &mergedLo, &mergedHi,
										 &ex->loKey, &ex->key))
		{
			page_range_widen(desc, &mergedLo, &mergedHi,
							 &ex->loKey, &ex->key);
			ex->valid = false;
			tbl->numValid--;
		}
	}

	/* Ensure remaining intervals for this tree are coalesced. */
	coalesce_page_intervals(desc, tbl, oids, oxid);

	slot = find_free_slot(tbl);
	while (slot < 0)
	{
		if (merge_closest_page_intervals(desc, tbl, oids, oxid))
		{
			slot = find_free_slot(tbl);
			continue;
		}

		/* Last resort: collapse to a tree-level lock. */
		remove_page_entries_for_tree(tbl, oids, oxid);
		slot = find_free_slot(tbl);
		if (slot < 0)
			return;

		e = &tbl->entries[slot];
		e->valid = true;
		e->oids = oids;
		e->oxid = oxid;
		e->level = OPredLockLevelTree;
		e->key.notNull = false;
		e->loKey.notNull = false;
		tbl->numValid++;
		return;
	}

	e = &tbl->entries[slot];
	e->valid = true;
	e->oids = oids;
	e->oxid = oxid;
	e->level = OPredLockLevelPage;
	e->key = mergedHi;
	e->loKey = mergedLo;
	tbl->numValid++;

	/* Maintain monotonic page intervals. */
	coalesce_page_intervals(desc, tbl, oids, oxid);
}

/* --------------------------------------------------------------------------
 * Public API
 * --------------------------------------------------------------------------
 */

/*
 * Acquire a predicate lock on curTuple (a leaf tuple in page, found by
 * pageFindContext).
 *
 * Must be called while the page is locked (inside a critical section is
 * fine), because we read the page highkey.
 */
void
o_pred_lock_acquire(BTreeDescr *desc,
					OTuple curTuple,
					Page page,
					OBTreeFindPageContext *context,
					OXid oxid)
{
	OPredLocksData *tbl;
	OPredLockKey pageHikey,
				pageLokey,
				tupleKey;
	bool		hiOK,
				loOK,
				keyOK;
	OTuple		lokeyTuple;
	int			slot;
	OPredLockEntry *e;
	int			samePage;

	Assert(OXidIsValid(oxid));
	Assert(MYPROCNUMBER >= 0 && MYPROCNUMBER < max_procs);

	tbl = &o_pred_locks[MYPROCNUMBER];

	/* Determine page boundaries. */
	hiOK = predlock_hikey_from_page(desc, page, &pageHikey);

	/*
	 * loKey: use the context's lokey if it was tracked, otherwise the page
	 * is the leftmost (lokey = -infinity).
	 */
	if (BTREE_PAGE_FIND_IS(context, LOKEY_EXISTS))
	{
		lokeyTuple = btree_find_context_lokey(context);
		loOK = predlock_key_from_tuple(desc, lokeyTuple, &pageLokey);
	}
	else
	{
		pageLokey.notNull = false;
		pageLokey.formatFlags = 0;
		pageLokey.len = 0;
		loOK = true;
	}

	/* Extract a non-leaf key from the current tuple for tuple-level locks. */
	{
		bool		key_palloc = false;
		OTuple		key;
		char		keyBuf[O_BTREE_MAX_KEY_SIZE];

		key = o_btree_tuple_make_key(desc, curTuple, keyBuf, false, &key_palloc);
		keyOK = predlock_key_from_tuple(desc, key, &tupleKey);
		if (key_palloc)
			pfree(key.data);
	}

	LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);

	/*
	 * Fast path: if we already have a lock that covers this key, we're done.
	 */
	if (keyOK)
	{
		OTuple		k = predlock_key_to_tuple(&tupleKey);

		if (already_locked(desc, tbl, desc->oids, oxid, k, &pageHikey, &pageLokey))
		{
			LWLockRelease(&tbl->lock);
			return;
		}
	}

	/*
	 * Try to add a tuple-level lock.  Promote to page-level if the key is
	 * too long, or if too many tuple locks already cover this page, or if
	 * the table is full.
	 */
	if (!keyOK || !hiOK || !loOK)
	{
		/* Key too long to store – add a page-level lock immediately. */
		if (hiOK && loOK)
			insert_page_lock(desc, tbl, desc->oids, oxid, &pageHikey, &pageLokey);
		/* else: hikey also too long – promote to tree level */
		else
		{
			/* Find or create a tree-level lock. */
			bool		found = false;
			int			i;

			for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
			{
				OPredLockEntry *ex = &tbl->entries[i];

				if (ex->valid && ex->level == OPredLockLevelTree &&
					ORelOidsIsEqual(ex->oids, desc->oids) &&
					ex->oxid == oxid)
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				slot = find_free_slot(tbl);
				if (slot >= 0)
				{
					e = &tbl->entries[slot];
					e->valid = true;
					e->oids = desc->oids;
					e->oxid = oxid;
					e->level = OPredLockLevelTree;
					e->key.notNull = false;
					e->loKey.notNull = false;
					tbl->numValid++;
				}
			}
		}
		LWLockRelease(&tbl->lock);
		return;
	}

	/* Count existing tuple-level locks for the same page. */
	samePage = count_tuple_entries_on_page(tbl, desc->oids, oxid, &pageHikey);

	if (samePage >= O_PRED_LOCK_PAGE_PROMOTE_THRESHOLD ||
		tbl->numValid >= O_PRED_LOCKS_MAX_ENTRIES)
	{
		/* Promote: replace tuple-level entries for this page with one page lock. */
		remove_tuple_entries_on_page(tbl, desc->oids, oxid, &pageHikey);
		insert_page_lock(desc, tbl, desc->oids, oxid, &pageHikey, &pageLokey);
		LWLockRelease(&tbl->lock);
		return;
	}

	/* Add a new tuple-level entry. */
	slot = find_free_slot(tbl);
	if (slot < 0)
	{
		/* No free slot – promote to page level. */
		remove_tuple_entries_on_page(tbl, desc->oids, oxid, &pageHikey);
		insert_page_lock(desc, tbl, desc->oids, oxid, &pageHikey, &pageLokey);
		LWLockRelease(&tbl->lock);
		return;
	}

	e = &tbl->entries[slot];
	e->valid = true;
	e->oids = desc->oids;
	e->oxid = oxid;
	e->level = OPredLockLevelTuple;
	e->key = tupleKey;
	/* loKey not meaningful for tuple-level; clear it. */
	e->loKey.notNull = false;
	e->loKey.formatFlags = 0;
	e->loKey.len = 0;
	tbl->numValid++;

	LWLockRelease(&tbl->lock);
}

/*
 * Check for a predicate lock conflict.
 *
 * Scans all backends' predicate lock tables.  For each entry:
 *  - If the owner is myOxid, update *lockStatus but do not report a conflict.
 *  - If the owner is a different in-progress transaction and the lock covers
 *    key/keyType with a mode that conflicts with lockMode, return true and set
 *    *conflictOxid.
 *  - Committed/aborted transactions' entries are ignored (they should have
 *    been cleaned up, but we handle them defensively).
 *
 * lockMode is the lock mode the caller wants to acquire (e.g. RowLockUpdate
 * for DELETE).  A predicate lock always uses RowLockKeyShare, so a conflict
 * exists when ROW_LOCKS_CONFLICT(RowLockKeyShare, lockMode).
 */
bool
o_pred_lock_check(BTreeDescr *desc,
				  OTuple key,
				  BTreeKeyType keyType,
				  RowLockMode lockMode,
				  OXid myOxid,
				  OXid *conflictOxid,
				  BTreeModifyLockStatus *lockStatus)
{
	int			proc;

	/* KeyShare only conflicts with Update (0+3>=3). */
	if (!ROW_LOCKS_CONFLICT(RowLockKeyShare, lockMode))
		return false;

	for (proc = 0; proc < max_procs; proc++)
	{
		OPredLocksData *tbl = &o_pred_locks[proc];
		int			i;

		if (tbl->numValid == 0)
			continue;

		LWLockAcquire(&tbl->lock, LW_SHARED);

		for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
		{
			OPredLockEntry *e = &tbl->entries[i];
			CommitSeqNo csn;

			if (!e->valid)
				continue;
			if (!ORelOidsIsEqual(e->oids, desc->oids))
				continue;
			if (!entry_covers_key(desc, key, keyType, e))
				continue;

			if (e->oxid == myOxid)
			{
				/* Self-lock: update lockStatus but not a conflict. */
				*lockStatus = Max(*lockStatus, BTreeModifyWeakerLock);
				continue;
			}

			/* Check whether the owning transaction is still in progress. */
			csn = oxid_get_csn(e->oxid, false);
			if (COMMITSEQNO_IS_INPROGRESS(csn))
			{
				*conflictOxid = e->oxid;
				LWLockRelease(&tbl->lock);
				return true;
			}
			/* Committed or aborted – treat as gone (cleanup will follow). */
		}

		LWLockRelease(&tbl->lock);
	}

	return false;
}

/*
 * Release all predicate lock entries owned by oxid.  Called when oxid's
 * transaction commits or aborts.
 */
void
o_pred_lock_release_all(OXid oxid)
{
	int			proc;

	for (proc = 0; proc < max_procs; proc++)
	{
		OPredLocksData *tbl = &o_pred_locks[proc];
		bool		hasAny = false;
		int			i;

		/* Quick check without locking. */
		if (tbl->numValid == 0)
			continue;

		/* Check if any entry belongs to oxid (shared lock first). */
		LWLockAcquire(&tbl->lock, LW_SHARED);
		for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES && !hasAny; i++)
		{
			if (tbl->entries[i].valid && tbl->entries[i].oxid == oxid)
				hasAny = true;
		}
		LWLockRelease(&tbl->lock);

		if (!hasAny)
			continue;

		/* Upgrade to exclusive lock to remove entries. */
		LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);
		for (i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
		{
			if (tbl->entries[i].valid && tbl->entries[i].oxid == oxid)
			{
				tbl->entries[i].valid = false;
				tbl->numValid--;
			}
		}
		LWLockRelease(&tbl->lock);
	}
}
