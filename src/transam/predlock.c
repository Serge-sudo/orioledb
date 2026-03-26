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

#include "access/htup_details.h"
#include "funcapi.h"
#include "tableam/handler.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

/* Global pointer to the per-backend predicate lock tables in shared memory. */
OPredLocksData *o_pred_locks = NULL;

#define PREDLOCK_OUTPUT_COLS 12
#define PREDLOCK_LEVEL_COUNT lengthof(predlock_level_names)

PG_FUNCTION_INFO_V1(orioledb_predicate_locks);

static const char *const predlock_level_names[] = {
	[OPredLockLevelTuple] = "tuple",
	[OPredLockLevelPage] = "page",
	[OPredLockLevelTree] = "tree"
};

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

/*
 * Try to interpret a stored predicate lock key as a signed integer value.
 *
 * @param key  Stored predicate lock key (must be not-null).
 * @param out  Set to the decoded int64 value on success.
 * @return true if the key length matches int16/int32/int64 and was decoded,
 *         false otherwise.
 */
static bool
predlock_key_to_int64(OPredLockKey *key, int64 *out)
{
	if (!key->notNull)
		return false;

	switch (key->len)
	{
		case sizeof(int16):
			{
				int16		v;

				memcpy(&v, key->data, sizeof(int16));
				*out = (int64) v;
				return true;
			}
		case sizeof(int32):
			{
				int32		v;

				memcpy(&v, key->data, sizeof(int32));
				*out = (int64) v;
				return true;
			}
		case sizeof(int64):
			{
				int64		v;

				memcpy(&v, key->data, sizeof(int64));
				*out = v;
				return true;
			}
		default:
			return false;
	}
}

/*
 * Get the inclusive low and high bounds for a predicate lock entry.
 *
 * For page-level locks the range is [loKey, key); for tuple-level entries (or
 * other granularities) the single key acts as both bounds.
 */
static void
predlock_entry_bounds(OPredLockEntry *e, OPredLockKey **lo, OPredLockKey **hi)
{
	if (e->level == OPredLockLevelPage)
	{
		*lo = &e->loKey;
		*hi = &e->key;
	}
	else
	{
		/* Tuple-level and other granularities use the single key as both ends. */
		*lo = &e->key;
		*hi = &e->key;
	}
}

/*
 * Return a small positive gap between two raw keys using lexicographic bytes.
 *
 * Assumes left < right lexicographically.  Uses the first differing byte (or
 * length difference) as a heuristic distance; capped to INT_MAX.  Returns 0
 * when keys are identical and INT_MAX when either key is NULL/unknown.
 */
static int
lexicographic_key_gap(OPredLockKey *left, OPredLockKey *right)
{
	int			distance;
	size_t		minlen;
	size_t		i;

	if (!left->notNull || !right->notNull)
		return INT_MAX;

	minlen = Min(left->len, right->len);

	for (i = 0; i < minlen; i++)
	{
		if (left->data[i] != right->data[i])
		{
			int			diff = (unsigned char) right->data[i] -
			(unsigned char) left->data[i];

			/* left < right should imply diff > 0. */
			Assert(diff > 0);
			return diff;
		}
	}

	/* Prefix case: all compared bytes match but lengths differ; gap by length. */
	if (right->len > left->len)
	{
		size_t		len_diff = right->len - left->len;

		return len_diff > (size_t) INT_MAX ? INT_MAX : (int) len_diff;
	}

	/* Equal-length identical keys: treat as zero additional gap. */
	return 0;
}

/*
 * Calculate a distance heuristic between two predicate lock entries belonging
 * to the same transaction and tree.
 *
 * For integer-like keys (int2/int4/int8) we use the numeric gap between the
 * ranges.  Overlapping or touching ranges have distance 0 (page ranges are
 * treated as half-open [lo, hi)).  For other datatypes we fall back to
 * comparator-based overlap detection with the same semantics.
 *
 * @param desc  B-tree descriptor for comparisons.
 * @param a,b   Predicate lock entries to compare (must belong to the same
 *              tree/transaction to produce a finite result).
 * @return      INT_MAX if entries are incompatible for merging, otherwise a
 *              non-negative distance where 0 means overlap/adjacent.
 */
static int
get_distance_between(BTreeDescr *desc, OPredLockEntry *a, OPredLockEntry *b)
{
	OPredLockKey *aLo,
			   *aHi,
			   *bLo,
			   *bHi;
	int64		aLoVal,
				aHiVal,
				bLoVal,
				bHiVal;
	bool		aLoNum,
				aHiNum,
				bLoNum,
				bHiNum;

	if (a->oxid != b->oxid)
		return INT_MAX;
	if (!ORelOidsIsEqual(a->oids, b->oids))
		return INT_MAX;
	if (a->level == OPredLockLevelTree || b->level == OPredLockLevelTree)
		return INT_MAX;

	predlock_entry_bounds(a, &aLo, &aHi);
	predlock_entry_bounds(b, &bLo, &bHi);

	aLoNum = predlock_key_to_int64(aLo, &aLoVal);
	aHiNum = predlock_key_to_int64(aHi, &aHiVal);
	bLoNum = predlock_key_to_int64(bLo, &bLoVal);
	bHiNum = predlock_key_to_int64(bHi, &bHiVal);

	if (aLoNum && aHiNum && bLoNum && bHiNum)
	{
		int64		gap;

		/* Normalize ordering to compute the gap. */
		if (aHiVal > bLoVal && bHiVal > aLoVal)
			gap = 0;			/* overlap */
		else if (aHiVal == bLoVal || bHiVal == aLoVal)
			gap = 0;			/* touch without overlap */
		else if (aHiVal < bLoVal)
			gap = bLoVal - aHiVal;
		else
			gap = aLoVal - bHiVal;

		if (gap > (int64) INT_MAX)
			return INT_MAX;
		return (int) gap;
	}

	/*
	 * Fallback heuristic using key ordering only.  If ranges overlap or touch,
	 * distance is zero; otherwise use 1 as a minimal positive gap so the
	 * caller can still prefer closer ranges.
	 */
	{
		const bool	isHi = true;
		const bool	isLo = false;
		int			cmp_a_hi_b_lo = predlock_cmp_stored_keys(desc, aHi, isHi, bLo, isLo);
		int			cmp_b_hi_a_lo = predlock_cmp_stored_keys(desc, bHi, isHi, aLo, isLo);

		if (cmp_a_hi_b_lo > 0 && cmp_b_hi_a_lo > 0)
			return 0;
		if (cmp_a_hi_b_lo == 0 || cmp_b_hi_a_lo == 0)
			return 0;

		if (cmp_a_hi_b_lo < 0)
			return lexicographic_key_gap(aHi, bLo);
		else
			return lexicographic_key_gap(bHi, aLo);
	}

	return 1;
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

		gapScore = get_distance_between(desc, left, right);

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
 * Count tuple-level entries for the same page (identified by hikey) that also
 * fall inside the page key range [pageLokey, pageHikey).  Used to decide
 * whether to promote.
 */
static int
count_tuple_entries_on_page(BTreeDescr *desc,
							OPredLocksData *tbl,
							ORelOids oids, OXid oxid,
							OPredLockKey *pageLokey,
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
		/* Must be within [pageLokey, pageHikey). */
		{
			OTuple		t = predlock_key_to_tuple(&e->key);

			if (predlock_cmp_with_lokey(desc, t, BTreeKeyNonLeafKey, pageLokey) < 0)
				continue;
			if (predlock_cmp_with_hikey(desc, t, BTreeKeyNonLeafKey, pageHikey) >= 0)
				continue;
		}

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
	samePage = count_tuple_entries_on_page(desc, tbl, desc->oids, oxid,
										   &pageLokey, &pageHikey);

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

typedef struct OPredLockSnapshotEntry
{
	ORelOids	oids;
	OXid		oxid;
	pid_t		pid;
	OPredLockLevel level;
	OPredLockKey key;
	OPredLockKey lokey;
} OPredLockSnapshotEntry;

static Jsonb *
predlock_key_to_jsonb_or_null(OPredLockKey *key, BTreeDescr *desc)
{
	JsonbValue *jsval;
	JsonbParseState *state = NULL;
	OTuple		tuple;

	if (!desc || !key->notNull)
		return NULL;

	tuple = predlock_key_to_tuple(key);
	jsval = o_btree_key_to_jsonb(desc, tuple, &state);
	if (!jsval)
		return NULL;

	return JsonbValueToJsonb(jsval);
}

static bytea *
predlock_key_to_raw(OPredLockKey *key)
{
	bytea	   *raw;

	if (!key->notNull || key->len == 0)
		return NULL;

	raw = (bytea *) palloc(key->len + VARHDRSZ);
	SET_VARSIZE(raw, key->len + VARHDRSZ);
	memcpy(VARDATA(raw), key->data, key->len);
	return raw;
}

static BTreeDescr *
predlock_entry_find_btree(OTableDescr *descr, ORelOids oids)
{
	OIndexNumber ixnum;

	if (descr == NULL)
		return NULL;

	ixnum = find_tree_in_descr(descr, oids);
	if (ixnum == InvalidIndexNumber)
		return NULL;
	if (ixnum == TOASTIndexNumber)
		return &descr->toast->desc;
	if (ixnum < descr->nIndices)
		return &descr->indices[ixnum]->desc;

	return NULL;
}

static List *
predlock_snapshot_locks(void)
{
	List	   *snapshot = NIL;

	for (int backend_index = 0; backend_index < max_procs; backend_index++)
	{
		OPredLocksData *tbl = &o_pred_locks[backend_index];
		int			entry_index;

		if (tbl->numValid == 0)
			continue;

		LWLockAcquire(&tbl->lock, LW_SHARED);
		for (entry_index = 0; entry_index < O_PRED_LOCKS_MAX_ENTRIES; entry_index++)
		{
			OPredLockEntry *e = &tbl->entries[entry_index];
			CommitSeqNo csn;
			OPredLockSnapshotEntry *copy;
			PGPROC	   *proc;

			if (!e->valid)
				continue;

			csn = oxid_get_csn(e->oxid, false);
			if (!COMMITSEQNO_IS_INPROGRESS(csn))
				continue;

			Assert(backend_index >= 0 && backend_index < MaxBackends);
			proc = GetPGProcByNumber(backend_index);

			copy = (OPredLockSnapshotEntry *) palloc(sizeof(OPredLockSnapshotEntry));
			copy->oids = e->oids;
			copy->oxid = e->oxid;
			copy->pid = (proc && proc->pid > 0) ? proc->pid : InvalidPid;
			copy->level = e->level;
			copy->key = e->key;
			copy->lokey = e->loKey;

			snapshot = lappend(snapshot, copy);
		}
		LWLockRelease(&tbl->lock);
	}

	return snapshot;
}

static void
predlock_emit_entry(OPredLockSnapshotEntry *e, TupleDesc tupdesc,
					Tuplestorestate *tupstore)
{
	Datum		values[PREDLOCK_OUTPUT_COLS];
	bool		nulls[PREDLOCK_OUTPUT_COLS];
	const char *level;
	OTableDescr *descr = NULL;
	BTreeDescr *desc = NULL;
	Jsonb	   *keyJson = NULL;
	Jsonb	   *lokeyJson = NULL;
	bytea	   *keyRaw = NULL;
	bytea	   *loKeyRaw = NULL;

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	if (e->level < PREDLOCK_LEVEL_COUNT)
		level = predlock_level_names[e->level];
	else
		level = "unknown";

	if (e->oids.datoid == MyDatabaseId)
	{
		/* Descriptor may be absent for dropped/unloaded relations. */
		descr = o_fetch_table_descr(e->oids);
		desc = predlock_entry_find_btree(descr, e->oids);
	}

	keyJson = predlock_key_to_jsonb_or_null(&e->key, desc);
	lokeyJson = predlock_key_to_jsonb_or_null(&e->lokey, desc);
	keyRaw = predlock_key_to_raw(&e->key);
	loKeyRaw = predlock_key_to_raw(&e->lokey);

	values[0] = ObjectIdGetDatum(e->oids.datoid);
	values[1] = ObjectIdGetDatum(e->oids.reloid);
	values[2] = ObjectIdGetDatum(e->oids.relnode);
	if (e->pid != InvalidPid)
		values[3] = Int32GetDatum(e->pid);
	else
		nulls[3] = true;
	values[4] = Int64GetDatum((int64) e->oxid);
	values[5] = CStringGetTextDatum(level);

	if (keyJson)
		values[6] = PointerGetDatum(keyJson);
	else
		nulls[6] = true;

	if (lokeyJson)
		values[7] = PointerGetDatum(lokeyJson);
	else
		nulls[7] = true;

	if (keyRaw)
		values[8] = PointerGetDatum(keyRaw);
	else
		nulls[8] = true;

	if (e->key.notNull)
		values[9] = Int16GetDatum(e->key.formatFlags);
	else
		nulls[9] = true;

	if (loKeyRaw)
		values[10] = PointerGetDatum(loKeyRaw);
	else
		nulls[10] = true;

	if (e->lokey.notNull)
		values[11] = Int16GetDatum(e->lokey.formatFlags);
	else
		nulls[11] = true;

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

/*
 * Expose currently held predicate locks for the current database.
 *
 * Each row reports tree identifiers (datoid, reloid, relnode), owning oxid,
 * lock level, JSON representations of keys when available, and raw key bytes
 * plus format flags for debugging/inspection.
 */
Datum
orioledb_predicate_locks(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	List	   *snapshot = NIL;
	ListCell   *lc;
	bool		randomAccess;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(PREDLOCK_OUTPUT_COLS);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "datoid", OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "reloid", OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "relnode", OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "oxid", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "lock_level", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "key", JSONBOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "lokey", JSONBOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 9, "key_raw", BYTEAOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 10, "key_format_flags", INT2OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 11, "lokey_raw", BYTEAOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 12, "lokey_format_flags", INT2OID, -1, 0);

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;

	tupstore = tuplestore_begin_heap(randomAccess,
									 false /* interXact */,
									 work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	snapshot = predlock_snapshot_locks();

	foreach(lc, snapshot)
		predlock_emit_entry((OPredLockSnapshotEntry *) lfirst(lc),
							tupdesc, tupstore);

	MemoryContextSwitchTo(oldcontext);

	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}
