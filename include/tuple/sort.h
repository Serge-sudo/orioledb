/*-------------------------------------------------------------------------
 *
 * sort.h
 * 		Declarations for implementation of orioledb tuple sorting
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tuple/sort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TUPLE_SORT_H
#define __TUPLE_SORT_H

#include "postgres.h"
#include "tableam/descr.h"

extern Tuplesortstate *tuplesort_begin_orioledb_index(OIndexDescr *idx,
													  int workMem,
													  bool randomAccess,
													  SortCoordinate coordinate);
extern Tuplesortstate *tuplesort_begin_orioledb_primary_rebuild(OIndexDescr *idx,
																int workMem,
																bool randomAccess,
																SortCoordinate coordinate);
extern Tuplesortstate *tuplesort_begin_orioledb_toast(OIndexDescr *toast,
													  OIndexDescr *primary,
													  int workMem,
													  bool randomAccess,
													  SortCoordinate coordinate);
extern OTuple tuplesort_getotuple(Tuplesortstate *state, bool forward);
extern bool tuplesort_getrebuildtuple(Tuplesortstate *state, bool forward,
									  OTuple *tuple, bytea **rowid,
									  int *rowid_len);
extern void tuplesort_putotuple(Tuplesortstate *state, OTuple tup);
extern void tuplesort_putrebuildtuple(Tuplesortstate *state, OTuple key,
									  bytea *rowid);

#endif							/* __TUPLE_SORT_H */
