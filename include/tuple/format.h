/*-------------------------------------------------------------------------
 *
 * format.h
 * 		Declarations for orioledb tuple format.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tuple/format.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TUPLE_FORMAT_H__
#define __TUPLE_FORMAT_H__

#include "postgres.h"

typedef struct
{
	TupleDesc	desc;
	char	   *tp;
	bits8	   *bp;
	uint32		off;
	uint16		attnum;
	uint16		natts;
	bool		hasnulls;
	bool		slow;
} OTupleReaderState;

typedef struct
{
	uint16		hasnulls:1,
				len:15;
	uint16		natts;
	uint32		version;
} OTupleHeaderData;

#define O_TUPLE_FLAGS_FIXED_FORMAT	0x1

typedef struct
{
	uint16		natts;
	uint16		len;
} OTupleFixedFormatSpec;

typedef OTupleHeaderData *OTupleHeader;
#define SizeOfOTupleHeader MAXALIGN(sizeof(OTupleHeaderData))

typedef struct BrigeData
{
	bool		is_pkey;
	ItemPointer bridge_iptr;
	/* compared with InvalidAttrNumber, so should be greater than 0 */
	AttrNumber	attnum;
} BrigeData;

/*
 * Works with orioledb table tuples in primary index. It can fetch
 * TOAST pointers from table tuple.
 */
#define o_fastgetattr(tup, attnum, tupleDesc, spec, isnull)			\
(																	\
	AssertMacro((attnum) > 0),										\
	(*(isnull) = false),											\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		((attnum) - 1 < (spec)->natts) ?							\
		(															\
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				fetchatt(TupleDescAttr((tupleDesc), (attnum) - 1),	\
					(char *) (tup).data +							\
					TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff) \
			)														\
			:														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec), (isnull)) \
		)															\
		:															\
		(															\
			(*(isnull) = true),										\
			(Datum) NULL											\
		)															\
	)																\
	:																\
	(																\
		(!(((OTupleHeader) (tup).data)->hasnulls)) ?				\
		(															\
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				fetchatt(TupleDescAttr((tupleDesc), (attnum)-1),	\
					(char *) (tup).data + SizeOfOTupleHeader +		\
					TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff) \
			)														\
			:														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec), (isnull)) \
		)															\
		:															\
		(															\
			att_isnull((attnum) - 1, (bits8 *) ((tup).data + SizeOfOTupleHeader)) ? \
			(														\
				(*(isnull) = true),									\
				(Datum) NULL										\
			)														\
			:														\
			(														\
				o_toast_nocachegetattr((tup), (attnum), (tupleDesc), (spec), (isnull)) \
			)														\
		)															\
	)																\
)

#define o_fastgetattr_ptr(tup, attnum, tupleDesc, spec)				\
(																	\
	AssertMacro((attnum) > 0),										\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		((attnum) - 1 < (spec)->natts) ?							\
		(															\
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				(char *) (tup).data +									\
				TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff \
			)														\
			:														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
		)															\
		:															\
		(															\
			NULL													\
		)															\
	)																\
	:																\
	(																\
		(!(((OTupleHeader) (tup).data)->hasnulls)) ?				\
		(															\
			TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff >= 0 ? \
			(														\
				(char *) (tup).data + SizeOfOTupleHeader +				\
				TupleDescAttr((tupleDesc), (attnum) - 1)->attcacheoff \
			)														\
			:														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
		)															\
		:															\
		(															\
			att_isnull((attnum) - 1, (bits8 *) ((tup).data + SizeOfOTupleHeader)) ? \
			(														\
				NULL												\
			)														\
			:														\
			(														\
				o_toast_nocachegetattr_ptr((tup), (attnum), (tupleDesc), (spec)) \
			)														\
		)															\
	)																\
)

#define o_tuple_size(tup, spec)										\
(																	\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		(spec)->len													\
	)																\
	:																\
	(																\
		((OTupleHeader) (tup).data)->len							\
	)																\
)

#define o_has_nulls(tup)											\
(																	\
	((tup).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) ?				\
	(																\
		false														\
	)																\
	:																\
	(																\
		((OTupleHeader) (tup).data)->hasnulls						\
	)																\
)

extern void o_tuple_init_reader(OTupleReaderState *state, OTuple tuple,
								TupleDesc desc, OTupleFixedFormatSpec *spec);

/* Hot path functions - inline for performance */
static inline Datum o_tuple_read_next_field(OTupleReaderState *state, bool *isnull);
static inline uint32 o_tuple_next_field_offset(OTupleReaderState *state,
												Form_pg_attribute att);

/* Non-inline versions for external linkage if needed */
extern Datum o_tuple_read_next_field_impl(OTupleReaderState *state, bool *isnull);
extern uint32 o_tuple_next_field_offset_impl(OTupleReaderState *state,
											  Form_pg_attribute att);
extern ItemPointer o_tuple_get_last_iptr(TupleDesc desc,
										 OTupleFixedFormatSpec *spec,
										 OTuple tuple, bool *isnull);
extern Datum o_toast_nocachegetattr(OTuple tuple, int attnum,
									TupleDesc tupleDesc,
									OTupleFixedFormatSpec *spec,
									bool *is_null);
extern Pointer o_toast_nocachegetattr_ptr(OTuple tuple, int attnum,
										  TupleDesc tupleDesc,
										  OTupleFixedFormatSpec *spec);
extern Pointer o_tuple_get_data(OTuple tuple, int *size, OTupleFixedFormatSpec *spec);
extern Size o_new_tuple_size(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
							 ItemPointer iptr, BrigeData *bridge_data, uint32 version,
							 Datum *values, bool *isnull, char *to_toast);
extern void o_tuple_fill(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						 OTuple *tuple, Size tuple_size,
						 ItemPointer iptr, BrigeData *bridge_data, uint32 version,
						 Datum *values, bool *isnull, char *to_toast);
extern OTuple o_form_tuple(TupleDesc tupleDesc, OTupleFixedFormatSpec *spec,
						   uint32 version, Datum *values, bool *isnull,
						   BrigeData *bridge_data);
extern uint32 o_tuple_get_version(OTuple tuple);
extern void o_tuple_set_version(OTupleFixedFormatSpec *spec, OTuple *tuple,
								uint32 version);
extern void o_tuple_set_ctid(OTuple tuple, ItemPointer iptr);

/*
 * Inline implementations of hot-path functions for performance optimization.
 * These functions are called frequently during tuple processing and inlining
 * them reduces function call overhead significantly.
 */

/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attstorage != 'p')

static inline uint32
o_tuple_next_field_offset(OTupleReaderState *state, Form_pg_attribute att)
{
	uint32		off;

	/*
	 * Fast path: use cached offset if available and we're not in slow mode.
	 * This is the most common case and should be optimized aggressively.
	 */
	if (likely(!state->slow && att->attcacheoff >= 0))
	{
		state->off = att->attcacheoff;
	}
	else if (att->attlen == -1)
	{
		/* Variable-length attribute */
		if (!state->slow &&
			state->off == att_align_nominal(state->off, att->attalign))
		{
			att->attcacheoff = state->off;
		}
		else
		{
			state->off = att_align_pointer(state->off, att->attalign, -1,
										   state->tp + state->off);
			state->slow = true;
		}
	}
	else
	{
		/* Fixed-length attribute */
		state->off = att_align_nominal(state->off, att->attalign);
		if (!state->slow)
			att->attcacheoff = state->off;
	}

	off = state->off;

	/*
	 * Update offset for next field. Check for toast pointers first as they
	 * have a fixed size.
	 */
	if (unlikely(!att->attbyval && att->attlen < 0 &&
				 IS_TOAST_POINTER(state->tp + state->off)))
	{
		state->off += sizeof(OToastValue);
	}
	else
	{
		state->off = att_addlength_pointer(state->off,
										   att->attlen,
										   state->tp + state->off);
	}

	if (att->attlen <= 0)
		state->slow = true;

	state->attnum++;

	return off;
}

static inline Datum
o_tuple_read_next_field(OTupleReaderState *state, bool *isnull)
{
	Form_pg_attribute att;
	Datum		result;
	uint32		off;

	/* Handle fields beyond stored attributes */
	if (unlikely(state->attnum >= state->natts))
	{
		Form_pg_attribute attr = &state->desc->attrs[state->attnum];

		if (attr->atthasmissing)
		{
			result = getmissingattr(state->desc,
									state->attnum + 1,
									isnull);
			state->attnum++;
			return result;
		}
		else
		{
			*isnull = true;
			state->attnum++;
			return (Datum) 0;
		}
	}

	att = TupleDescAttr(state->desc, state->attnum);

	/* Check for null value */
	if (unlikely(state->hasnulls && att_isnull(state->attnum, state->bp)))
	{
		*isnull = true;
		state->slow = true;
		state->attnum++;
		return (Datum) 0;
	}

	*isnull = false;
	off = o_tuple_next_field_offset(state, att);

	return fetchatt(att, state->tp + off);
}

#endif							/* __TUPLE_FORMAT_H__ */
