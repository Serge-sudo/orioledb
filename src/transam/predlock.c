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

static int
count_page_entries_for_tree(OPredLocksData *tbl,
							ORelOids oids, OXid oxid)
{
	int			count = 0;
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
 * Insert a page-level lock entry into tbl.  If tbl is full, promote to a
 * tree-level lock.  Returns the slot used, or -1 if promoted to tree level
 * (which means the entry is now a tree-level lock in some free slot).
 */
static void
insert_page_lock(OPredLocksData *tbl,
				 ORelOids oids, OXid oxid,
				 OPredLockKey *pageHikey, OPredLockKey *pageLokey)
{
	int			slot;
	OPredLockEntry *e;

	/* Check if a page-level lock for this exact page already exists. */
	for (int i = 0; i < O_PRED_LOCKS_MAX_ENTRIES; i++)
	{
		OPredLockEntry *ex = &tbl->entries[i];

		if (!ex->valid || ex->level != OPredLockLevelPage)
			continue;
		if (!ORelOidsIsEqual(ex->oids, oids) || ex->oxid != oxid)
			continue;
		/* Same hikey? */
		if (ex->key.notNull == pageHikey->notNull &&
			(!pageHikey->notNull ||
			 (ex->key.len == pageHikey->len &&
			  ex->key.formatFlags == pageHikey->formatFlags &&
			  memcmp(ex->key.data, pageHikey->data, pageHikey->len) == 0)))
			return;				/* already have this page lock */
	}

	/* Possibly promote many page locks to a tree lock. */
	if (count_page_entries_for_tree(tbl, oids, oxid) >=
		O_PRED_LOCK_TREE_PROMOTE_THRESHOLD)
	{
		/* Promote: remove all page-level entries for this tree. */
		remove_page_entries_for_tree(tbl, oids, oxid);

		slot = find_free_slot(tbl);
		if (slot < 0)
		{
			/*
			 * Still full – this shouldn't happen since we just freed
			 * entries, but be safe.
			 */
			return;
		}
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

	slot = find_free_slot(tbl);
	if (slot < 0)
	{
		/* Table is full. Promote to tree lock immediately. */
		remove_page_entries_for_tree(tbl, oids, oxid);
		slot = find_free_slot(tbl);
		if (slot < 0)
			return;				/* still no room – shouldn't happen */

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
	e->key = *pageHikey;
	e->loKey = *pageLokey;
	tbl->numValid++;
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
			insert_page_lock(tbl, desc->oids, oxid, &pageHikey, &pageLokey);
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
		insert_page_lock(tbl, desc->oids, oxid, &pageHikey, &pageLokey);
		LWLockRelease(&tbl->lock);
		return;
	}

	/* Add a new tuple-level entry. */
	slot = find_free_slot(tbl);
	if (slot < 0)
	{
		/* No free slot – promote to page level. */
		remove_tuple_entries_on_page(tbl, desc->oids, oxid, &pageHikey);
		insert_page_lock(tbl, desc->oids, oxid, &pageHikey, &pageLokey);
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
