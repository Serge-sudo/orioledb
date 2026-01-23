/*-------------------------------------------------------------------------
 *
 * reindex_concurrent_hook.c
 *		Hook for synchronizing orioledb system trees during REINDEX CONCURRENTLY
 *
 * PostgreSQL's REINDEX CONCURRENTLY goes through multiple stages where it
 * updates index metadata in system catalogs (pg_index). Orioledb maintains
 * its own metadata in system trees (SYS_TREES_O_INDICES) which needs to be
 * kept synchronized during these stages.
 *
 * The main stages that require synchronization are:
 * 1. Index creation (handled by o_define_index)
 * 2. Setting index state flags (indisready, indislive, indisvalid)
 * 3. Index swap (swapping relfilenodes between old and new index)
 * 4. Cleanup (removing old concurrent index structure)
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/reindex_concurrent_hook.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "transam/oxid.h"

#include "access/relation.h"
#include "catalog/index.h"
#include "utils/rel.h"

/*
 * States for REINDEX CONCURRENTLY that we need to synchronize
 * These correspond to the index state flags in pg_index
 */
typedef enum
{
	REINDEX_CONCURRENT_SET_DEAD,	/* Mark old index as dead (indislive=false) */
	REINDEX_CONCURRENT_SWAP_RELFILENODES, /* Swap relfilenodes */
	REINDEX_CONCURRENT_SET_NEW_VALID, /* Mark new index as valid (indisvalid=true) */
	REINDEX_CONCURRENT_SET_OLD_INVALID, /* Mark old index as invalid */
	REINDEX_CONCURRENT_CLEANUP	/* Remove old index structure */
} ReindexConcurrentStage;

/*
 * orioledb_reindex_concurrent_hook
 *
 * Hook function to synchronize orioledb system trees during REINDEX CONCURRENTLY.
 * This function should be called from PostgreSQL's reindex concurrent logic at
 * appropriate stages.
 *
 * Parameters:
 *   heapRelation - The table being reindexed
 *   oldIndex - The old index relation (before swap)
 *   newIndex - The new concurrent index relation (with _ccnew suffix)
 *   stage - Which stage of REINDEX CONCURRENTLY we're in
 *
 * Note: This function handles orioledb-specific metadata updates but does NOT
 * modify PostgreSQL's system catalogs - that is PostgreSQL's responsibility.
 */
void
orioledb_reindex_concurrent_hook(Relation heapRelation,
								  Relation oldIndex,
								  Relation newIndex,
								  ReindexConcurrentStage stage)
{
	OTable	   *o_table;
	ORelOids	oids;
	OIndexNumber old_ix_num = InvalidIndexNumber;
	OIndexNumber new_ix_num = InvalidIndexNumber;
	int			i;
	OXid		oxid = get_current_oxid();
	CommitSeqNo csn = COMMITSEQNO_INPROGRESS;

	/* Only process orioledb tables */
	if (!is_orioledb_rel(heapRelation))
		return;

	ORelOidsSetFromRel(oids, heapRelation);
	o_table = o_tables_get(oids);
	if (o_table == NULL)
		return;

	/*
	 * Find the old and new index structures in o_table->indices
	 * We identify them by reloid (index OID) and relfilenode
	 */
	for (i = 0; i < o_table->nindices; i++)
	{
		if (o_table->indices[i].oids.reloid == oldIndex->rd_rel->oid)
		{
			/* Old index: matches old reloid */
			old_ix_num = i;
		}

		if (o_table->indices[i].oids.reloid == newIndex->rd_rel->oid)
		{
			/* New index: matches new reloid (temporary _ccnew index) */
			new_ix_num = i;
		}
	}

	/*
	 * Process the requested stage
	 */
	switch (stage)
	{
		case REINDEX_CONCURRENT_SET_DEAD:
			/*
			 * Stage 1: Mark old index as "dead" (indislive = false)
			 * In orioledb, we don't have a direct equivalent to indislive,
			 * but we need to ensure the metadata reflects that the old index
			 * is being phased out.
			 *
			 * For now, we just log this stage. The actual enforcement happens
			 * through PostgreSQL's planner which won't use dead indexes.
			 */
			if (old_ix_num != InvalidIndexNumber)
			{
				elog(DEBUG1, "orioledb_reindex_concurrent_hook: "
					 "Stage SET_DEAD for index reloid %u",
					 o_table->indices[old_ix_num].oids.reloid);
				
				/*
				 * Update the index metadata to reflect any changes.
				 * Note: The OIndex structure doesn't have explicit
				 * indislive/indisready flags, as orioledb relies on
				 * PostgreSQL's catalogs for visibility control.
				 */
				o_indices_update(o_table, old_ix_num, oxid, csn);
			}
			break;

		case REINDEX_CONCURRENT_SWAP_RELFILENODES:
			/*
			 * Stage 2: Swap relfilenodes between old and new index
			 * This is the critical stage where the new index takes over
			 * the identity of the old index.
			 *
			 * After PostgreSQL swaps the relfilenodes in pg_class:
			 * - oldIndex->rd_rel->relfilenode now points to the new data
			 * - newIndex->rd_rel->relfilenode now points to the old data
			 *
			 * We need to update orioledb's system trees to reflect this swap.
			 */
			if (old_ix_num != InvalidIndexNumber && new_ix_num != InvalidIndexNumber)
			{
				Oid			temp_relfilenode;

				elog(DEBUG1, "orioledb_reindex_concurrent_hook: "
					 "Stage SWAP_RELFILENODES: old index reloid %u (relfilenode %u), "
					 "new index reloid %u (relfilenode %u)",
					 o_table->indices[old_ix_num].oids.reloid,
					 o_table->indices[old_ix_num].oids.relnode,
					 o_table->indices[new_ix_num].oids.reloid,
					 o_table->indices[new_ix_num].oids.relnode);

				/*
				 * Swap the relfilenodes in our structures to match what
				 * PostgreSQL just did in pg_class.
				 */
				temp_relfilenode = o_table->indices[old_ix_num].oids.relnode;
				o_table->indices[old_ix_num].oids.relnode = oldIndex->rd_rel->relfilenode;
				o_table->indices[new_ix_num].oids.relnode = newIndex->rd_rel->relfilenode;

				/* Update both indices in the system tree */
				o_indices_update(o_table, old_ix_num, oxid, csn);
				o_indices_update(o_table, new_ix_num, oxid, csn);

				elog(DEBUG1, "orioledb_reindex_concurrent_hook: "
					 "After swap: old index relfilenode %u, new index relfilenode %u",
					 o_table->indices[old_ix_num].oids.relnode,
					 o_table->indices[new_ix_num].oids.relnode);
			}
			break;

		case REINDEX_CONCURRENT_SET_NEW_VALID:
			/*
			 * Stage 3: Mark new index as valid (indisvalid = true)
			 * The new index (which now has the old index's reloid after swap)
			 * is marked as valid and ready for use.
			 */
			if (old_ix_num != InvalidIndexNumber)
			{
				elog(DEBUG1, "orioledb_reindex_concurrent_hook: "
					 "Stage SET_NEW_VALID for index reloid %u",
					 o_table->indices[old_ix_num].oids.reloid);
				
				/* Update metadata to reflect the index is now valid */
				o_indices_update(o_table, old_ix_num, oxid, csn);
			}
			break;

		case REINDEX_CONCURRENT_SET_OLD_INVALID:
			/*
			 * Stage 4: Mark old index (now with temporary reloid) as invalid
			 * This prepares it for final cleanup.
			 */
			if (new_ix_num != InvalidIndexNumber)
			{
				elog(DEBUG1, "orioledb_reindex_concurrent_hook: "
					 "Stage SET_OLD_INVALID for index reloid %u",
					 o_table->indices[new_ix_num].oids.reloid);
				
				o_indices_update(o_table, new_ix_num, oxid, csn);
			}
			break;

		case REINDEX_CONCURRENT_CLEANUP:
			/*
			 * Stage 5: Final cleanup - remove old index structure
			 * This is handled by the existing cleanup function.
			 */
			elog(DEBUG1, "orioledb_reindex_concurrent_hook: "
				 "Stage CLEANUP - delegating to cleanup function");
			
			/*
			 * The cleanup function expects the "new" index relation
			 * to have the updated relfilenode from the swap.
			 * After the swap, oldIndex points to what was the new data.
			 */
			orioledb_index_validate_cleanup_old_concurrent(heapRelation, oldIndex);
			break;

		default:
			elog(WARNING, "orioledb_reindex_concurrent_hook: unknown stage %d", stage);
			break;
	}

	o_table_free(o_table);
}

/*
 * orioledb_reindex_concurrent_set_dead
 *
 * Convenience wrapper for stage 1: mark old index as dead
 */
void
orioledb_reindex_concurrent_set_dead(Relation heapRelation,
									  Relation oldIndex,
									  Relation newIndex)
{
	orioledb_reindex_concurrent_hook(heapRelation, oldIndex, newIndex,
									  REINDEX_CONCURRENT_SET_DEAD);
}

/*
 * orioledb_reindex_concurrent_swap
 *
 * Convenience wrapper for stage 2: swap relfilenodes
 * Call this AFTER PostgreSQL has swapped the relfilenodes in pg_class
 */
void
orioledb_reindex_concurrent_swap(Relation heapRelation,
								  Relation oldIndex,
								  Relation newIndex)
{
	orioledb_reindex_concurrent_hook(heapRelation, oldIndex, newIndex,
									  REINDEX_CONCURRENT_SWAP_RELFILENODES);
}

/*
 * orioledb_reindex_concurrent_set_valid
 *
 * Convenience wrapper for stage 3: mark new index as valid
 */
void
orioledb_reindex_concurrent_set_valid(Relation heapRelation,
									   Relation oldIndex,
									   Relation newIndex)
{
	orioledb_reindex_concurrent_hook(heapRelation, oldIndex, newIndex,
									  REINDEX_CONCURRENT_SET_NEW_VALID);
}

/*
 * orioledb_reindex_concurrent_set_invalid
 *
 * Convenience wrapper for stage 4: mark old index as invalid
 */
void
orioledb_reindex_concurrent_set_invalid(Relation heapRelation,
										 Relation oldIndex,
										 Relation newIndex)
{
	orioledb_reindex_concurrent_hook(heapRelation, oldIndex, newIndex,
									  REINDEX_CONCURRENT_SET_OLD_INVALID);
}

/*
 * orioledb_reindex_concurrent_cleanup
 *
 * Convenience wrapper for stage 5: cleanup old index structure
 */
void
orioledb_reindex_concurrent_cleanup(Relation heapRelation,
									 Relation oldIndex,
									 Relation newIndex)
{
	orioledb_reindex_concurrent_hook(heapRelation, oldIndex, newIndex,
									  REINDEX_CONCURRENT_CLEANUP);
}
