#ifndef _ptnk_types_h_
#define _ptnk_types_h_

#include <stddef.h>
#include <stdint.h>

/* these types are global when compiled with C */
#ifdef __cplusplus
namespace ptnk {

typedef uint64_t page_id_t;

#endif
#ifdef PTNK_ADD_PREFIX
#define P_(x) PTNK_##x
#else
#define P_(x) x
#endif

enum
{
	/*! enable write */
	P_(OWRITER) = 1 << 0,

	/*! create file if not exist */
	P_(OCREATE) = 1 << 1, 
	/*! start from scratch / throw away existing data */
	P_(OTRUNCATE) = 1 << 2,

	/*! automatically sync file at end of transaction */
	/*!
	 *	@note required if you want ptnk to be ACI"D"
	 */
	P_(OAUTOSYNC) = 1 << 3,

	/*! db files are partitioned */
	P_(OPARTITIONED) = 1 << 4,

	P_(ODEFAULT) = P_(OWRITER) | P_(OCREATE) | P_(OAUTOSYNC) | P_(OPARTITIONED),
};

enum put_mode_t
{
	P_(PUT_INSERT),
	P_(PUT_UPDATE),
	P_(PUT_LEAVE_EXISTING)
};

enum query_type_t
{
	P_(F_EXACT) = 0x1, /*!< match must be exact (internal flag) */
	P_(F_NEIGH) = 0x2, /*!< queries next record (not exact matching record, internal flag) */
	P_(F_LOWER_BOUND) = 0x4, /*!< query is processed by (idx_)lower_bound (internal flag) */
	P_(F_NOSEARCH) = 0x8, /*!< query involving no binary search (internal flag) */
	P_(F_NOQUERYLEAF) = 0x10, /*!< don't query leaf (cur->idx will be invalid) */
	
	P_(MATCH_EXACT) = P_(F_EXACT) | P_(F_LOWER_BOUND), /*!< find exact matching key */
	P_(MATCH_OR_PREV) = P_(F_LOWER_BOUND),
	P_(MATCH_OR_NEXT) = 0,
	P_(BEFORE) = P_(F_NEIGH) | P_(F_LOWER_BOUND),
	P_(AFTER) = P_(F_NEIGH),
	/* PREFIX, PREFIX_LAST */

	P_(FRONT) = P_(F_NOSEARCH) + 0, /*!< get first record in the table */
	P_(BACK) = P_(F_NOSEARCH) + 1, /*!< get last record in the table */

	P_(MATCH_EXACT_NOLEAF) = P_(MATCH_EXACT) | P_(F_NOQUERYLEAF),
};

#undef P_
#ifdef __cplusplus
} /* end of namespace ptnk */
#endif

typedef uint32_t ptnk_opts_t;

#endif /* _ptnk_types_h_ */
