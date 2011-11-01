#ifndef _ptnk_types_h_
#define _ptnk_types_h_

#include <stddef.h>
#include <stdint.h>

/* these types are global when compiled with C */
#ifdef __cplusplus
/* >>>>> begin C++ only types */
#include <set>
#include <vector>
namespace ptnk {

//                          0123456789abcdef
#define PGID_LOCALID_MASK 0x000FFFFFFFFFFFFFULL
#define PGID_LOCALID(pgid) ((pgid) & PGID_LOCALID_MASK)
#define PGID_PARTID(pgid) ((pgid) >> 52)
#define PGID_PARTSTART(partid) (((page_id_t)(partid)) << 52)
#define PGID_PARTLOCAL(partid, localid) (PGID_PARTSTART(partid) | ((localid) & PGID_LOCALID_MASK))
typedef uint64_t page_id_t;
typedef std::set<page_id_t> Spage_id_t;
typedef std::vector<page_id_t> Vpage_id_t;

#define PGID_INVALID (page_id_t)(~0)

//! partition local page id type
typedef uint64_t local_pgid_t;
#define PTNK_LOCALID_INVALID ((local_pgid_t)~0ULL)

//! partition id type
typedef uint16_t part_id_t;
//    the max valid partid is 4094 = 0xFFE to avoid being PGID_INVALID
#define PTNK_PARTID_MAX 4094
#define PTNK_PARTID_INVALID ((part_id_t)~0)

//! transaction ID number
/*!
 *	transaction unique identifier.
 *	Note: The ID is in order of transaction commit. 
 */
typedef uint64_t tx_id_t;
#define TXID_INVALID (tx_id_t)(~0)

typedef std::set<tx_id_t> Stx_id_t;

/* <<<<< end begin C++ only types */
#endif
#undef P_
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

	/*! spawn helper thread for background page alloc / compaction */
	/*!
	 *	@note The helper thread is not required for ptnk to work.
	 *	      Only for additional performance improvements (although it is huge).
	 */
	P_(OHELPERTHREAD) = 1 << 5,

	P_(ODEFAULT) = P_(OWRITER) | P_(OCREATE) | P_(OAUTOSYNC) | P_(OPARTITIONED) | P_(OHELPERTHREAD),
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
