#include <iostream>

#include "../ptnk/bitvector.h"
#include "../ptnk/page.h"

namespace ptnk
{

typedef tx_id_t ver_t;

namespace stm
{

enum { TPIO_NHASH = 64 };

struct OvrEntry
{
	page_id_t pgidOrig;

	page_id_t pgidOvr;

	ver_t ver;

	OvrEntry* prev;
};

inline int pgidhash(page_id_t pgid)
{
	return pgid % TPIO_NHASH;
}

class PgidBloomFilter
{
public:
	PgidBloomFilter()
	{
		::memset(m_bvBloomLocalOvrs, 0, sizeof(m_bvBloomLocalOvrs));	
	}

	void add(page_id_t pgid) {}
	bool mayContain(page_id_t pgid) { return true; }
	bool mayContain(const PgidBloomFilter& o) { return true; }

private:
	char m_bvBloomLocalOvrs[16];
};

class __attribute__ ((aligned (8))) LocalOvr
{
public:
	page_id_t searchOvr(page_id_t pgid);
	void newOvr(page_id_t pgidOrig, page_id_t pgidOvr);

private:
	enum { TAG_TXVER_LOCAL = 0 };

	std::vector<OvrEntry> m_ovrsLocal;

	OvrEntry* m_hashOvrs[TPIO_NHASH];

	PgidBloomFilter m_bfOvrs;

	//! tx ver id of read snapshot
	ver_t m_verRead;

	//! tx ver id of this tx
	ver_t m_verWrite;

	friend class ActiveOvr;
	bool checkConflict(LocalOvr* other);
	LocalOvr* m_prev;
	bool m_mergeOngoing;
	bool m_merged;
};

class ActiveOvr
{
public:
	bool tryCommit(LocalOvr* lovr);

private:
	void merge(LocalOvr* lovr);

	OvrEntry* m_hashOvrs[TPIO_NHASH];

	//! latest verified tx
	/*!
	 *	tx in this linked-list are ensured that they do not conflict each other
	 */
	LocalOvr* m_lovrVerifiedTip;
};

page_id_t
LocalOvr::searchOvr(page_id_t pgid)
{
	int h = pgidhash(pgid);
	OvrEntry* e = m_hashOvrs[h];

	while(e)
	{
		if(e->ver > m_verRead)
		{
			continue;
		}

		if(e->pgidOrig == pgid)
		{
			return e->pgidOvr;
		}

		e = e->prev;
	}

	return pgid; // no actie ovr pg found
}

void
LocalOvr::newOvr(page_id_t pgidOrig, page_id_t pgidOvr)
{
	// add entry to m_ovrsLocal
	{
		OvrEntry e;
		e.pgidOrig = pgidOrig;
		e.pgidOvr = pgidOvr;
		e.ver = TAG_TXVER_LOCAL;
		m_ovrsLocal.push_back(e);
	}

	// set up hash
	{
		OvrEntry* e = &m_ovrsLocal.back();

		int h = pgidhash(pgidOrig);
		e->prev = m_hashOvrs[h];
		m_hashOvrs[h] = e;
	}

	// add entry to bloom filter
	m_bfOvrs.add(pgidOrig);
}

bool
LocalOvr::checkConflict(LocalOvr* other)
{
	if(! other->m_bfOvrs.mayContain(m_bfOvrs)) return false;
	
	const int iE = m_ovrsLocal.size();
	const int jE = other->m_ovrsLocal.size();
	for(int i = 0; i < iE; ++ i)
	{
		page_id_t pgid = m_ovrsLocal[i].pgidOrig;

		if(! other->m_bfOvrs.mayContain(pgid))
		{
			// bloom filter ensures that pgid does not conflict w/ other->m_ovrsLocal...

			// skip
		}
		else
		{
			for(int j = 0; j < jE; ++ j)
			{
				page_id_t pgidO = other->m_ovrsLocal[j].pgidOrig;

				if(pgid == pgidO) return true;
			}
		}
	}

	return false;
}

bool
ActiveOvr::tryCommit(LocalOvr* lovr)
{
	// step 1: validate _lovr_ that it does not conflict with other txs and add _lovr_ to validated ovrs list
	{
		LocalOvr* lovrVerified = NULL;

		for(;;)
		{
			LocalOvr* lovrPrev = m_lovrVerifiedTip;
			lovr->m_prev = lovrPrev;
			lovr->m_verWrite = lovr->m_prev->m_verWrite + 1;
			__sync_synchronize(); // lovr->m_prev must be set BEFORE tip ptr CAS swing below

			for(LocalOvr* lovrBefore = lovrPrev; lovrBefore && lovrBefore != lovrVerified; lovrBefore = lovrBefore->m_prev)
			{
				if(lovr->m_verRead >= lovrBefore->m_verWrite)
				{
					break;
				}

				if(! lovr->checkConflict(lovrBefore))
				{
					// abort commit
					delete lovr;
					return false;
				}
			}
			lovrVerified = lovr->m_prev;

			if(__sync_bool_compare_and_swap(&m_lovrVerifiedTip, lovrPrev, lovr))
			{
				// CAS succeed and _lovr_ has been successfully added to list of verified txs...
				
				break; // step 1 done!
			}
			else
			{
				// some other unverified tx has been added to the list...

				// RETRY!
			}
		}
	}

	// step 2: merge _lovr_
	//         if there are any not yet merged txs, apply that first
	{
		// step 2.1: create vector of un-merged txs 
		std::vector<LocalOvr*> lovrsUnmerged;
		for(LocalOvr* o = lovr; o && !o->m_merged; o = o->m_prev)
		{
			lovrsUnmerged.push_back(o);
		}

		// step 2.2: merge txs older one first
		typedef std::vector<LocalOvr*>::const_reverse_iterator it_t;
		for(it_t it = lovrsUnmerged.rbegin(); it != lovrsUnmerged.rend(); ++ it)
		{
			LocalOvr* o = *it;

			if(o->m_merged)	continue; // skip merged tx
			merge(o);
		}
	}

	return true;
}

void
ActiveOvr::merge(LocalOvr* lovr)
{
	// FIXME: really implement the concurrent merge
	if(! __sync_bool_compare_and_swap(&lovr->m_mergeOngoing, false, true))
	{
		// wait until merge complete
		while(! lovr->m_merged)
		{
			 asm volatile("": : :"memory"); // force re-read
		}

		return;
	}

	const ver_t verWrite = lovr->m_verWrite;

	// int off = rand();
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		int roti = i; //(i + off) % TPIO_NHASH;

		// find the head entry of local ovrs and fill e->ver fields
		OvrEntry* e = lovr->m_hashOvrs[roti];
		for(; e && e->ver == LocalOvr::TAG_TXVER_LOCAL; e = e->prev)
		{
			e->ver = lovr->m_verWrite;
		}

		// append the local ovrs list to the current list
		e->prev = m_hashOvrs[roti];
		__sync_synchronize();

		m_hashOvrs[roti] = e->prev;
	}

	__sync_synchronize(); // m_merged must be set after actual merge finishes
	lovr->m_merged = true;
}

} // end of namespace stm

} // end of namespace ptnk

using namespace ptnk;
using namespace ptnk::stm;

int
main(int argc, char* argv[])
{
	return 0;
}
