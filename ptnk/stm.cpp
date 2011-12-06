#include "stm.h"
#include "exceptions.h"
#include "sysutils.h"

namespace ptnk
{

std::ostream&
operator<<(std::ostream& s, const OvrEntry& e)
{
	s << "[orig " << e.pgidOrig << " -> ovr " << e.pgidOvr << " | ver: " << e.ver << " prev: " << (void*)e.prev << "]";
	return s;
}

LocalOvr::LocalOvr(OvrEntry* hashOvrs[], ver_t verRead, page_id_t pgidStartPage)
:	m_pgidStartPageOrig(pgidStartPage),
	m_pgidStartPage(pgidStartPage),
	m_verRead(verRead), m_verWrite(0),
	m_prev(NULL),
	m_mergeOngoing(false), m_merged(false),
	m_bTerminator(false)
{
	if(hashOvrs)
	{
		for(int i = 0; i < TPIO_NHASH; ++ i)
		{
			m_hashOvrs[i] = hashOvrs[i];
		}
	}
	else
	{
		for(int i = 0; i < TPIO_NHASH; ++ i)
		{
			m_hashOvrs[i] = nullptr;
		}
	}
}

LocalOvr::~LocalOvr()
{
	if(! m_merged)
	{
		// OvrEntry ownership not transferred to ActiveOvr...

		// need to delete "OvrEntry"s this has created

		for(int i = 0; i < TPIO_NHASH; ++ i)
		{
			for(OvrEntry* e = m_hashOvrs[i]; e && e->ver == LocalOvr::TAG_TXVER_LOCAL;)
			{
				OvrEntry* prev = e->prev;
				delete e;
				e = prev;
			}
		}
	}
}

pair<page_id_t, ovr_status_t>
LocalOvr::searchOvr(page_id_t pgid)
{
	int h = pgidhash(pgid);

	for(OvrEntry* e = m_hashOvrs[h]; e; e = e->prev)
	{
		if(e->ver > m_verRead)
		{
			// ovr entry is newer than read snapshot
			continue;
		}

		if(e->pgidOrig == pgid)
		{
			ovr_status_t st = (e->ver == TAG_TXVER_LOCAL) ? OVR_LOCAL : OVR_GLOBAL;
			return make_pair(e->pgidOvr, st);
		}
	}

	return make_pair(pgid, OVR_NONE); // no actie ovr pg found
}

void
LocalOvr::addOvr(page_id_t pgidOrig, page_id_t pgidOvr)
{
	m_pgidOrigs.push_back(pgidOrig);

	// set up hash
	{
		// referencing to elem inside vector is unsafe (realloc will change its addr)
		OvrEntry* e = new OvrEntry;
		e->pgidOrig = pgidOrig;
		e->pgidOvr = pgidOvr;
		e->ver = TAG_TXVER_LOCAL;

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
	if(other->m_pgidStartPage != m_pgidStartPageOrig)
	{
		std::cerr << "aborting conflict check as start pg is different" << std::endl;
		return false;
	}

	if(! other->m_bfOvrs.mayContain(m_bfOvrs))
	{
		// a bloomfilter ensures that there are no conflict.
		return false;
	}
	
	const int iE = m_pgidOrigs.size();
	const int jE = other->m_pgidOrigs.size();
	for(int i = 0; i < iE; ++ i)
	{
		const page_id_t pgid = m_pgidOrigs[i];

		if(! other->m_bfOvrs.mayContain(pgid))
		{
			// bloom filter ensures that pgid does not conflict w/ other->m_pgidOrigs...

			// skip
		}
		else
		{
			for(int j = 0; j < jE; ++ j)
			{
				const page_id_t pgidO = other->m_pgidOrigs[j];

				if(pgid == pgidO) return true;
			}
		}
	}

	return false;
}

void
LocalOvr::filterConflict(LocalOvr* other)
{
	if(other->m_pgidStartPage != m_pgidStartPageOrig)
	{
		std::cerr << "aborting conflict check as start pg is different" << std::endl;
		return;
	}

	if(! other->m_bfOvrs.mayContain(m_bfOvrs))
	{
		// a bloomfilter ensures that there are no conflict.
		return;
	}
	
	PTNK_THROW_LOGIC_ERR("FIXME: not yet impl.");
}

void
LocalOvr::dump(std::ostream& s) const
{
	s << "** LocalOvr Dump ***" << std::endl;
	s << "verRead: " << m_verRead << " verWrite: " << m_verWrite << std::endl;
	s << "mergeOngoing: " << m_mergeOngoing << " merged: " << m_merged << std::endl;
	s << "* m_pgidOrigs dump" << std::endl;
	for(auto it = m_pgidOrigs.begin(); it != m_pgidOrigs.end(); ++ it)
	{
		s << *it << std::endl;
	}
}

ActiveOvr::ActiveOvr(page_id_t pgidStartPage, ver_t verBase)
:	m_pgidStartPage(pgidStartPage),	
	m_verBase(verBase),
	m_lovrVerifiedTip(NULL)
{
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		m_hashOvrs[i] = NULL;
	}
}

ActiveOvr::~ActiveOvr()
{
	for(LocalOvr* lovr = m_lovrVerifiedTip; lovr;)
	{
		LocalOvr* prev = lovr->m_prev;
		if(lovr->isMerged() || lovr->m_bTerminator)
		{
			delete lovr;
		}
		lovr = prev;
	}

	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		for(OvrEntry* e = m_hashOvrs[i]; e;)
		{
			OvrEntry* prev = e->prev;
			delete e;
			e = prev;
		}
	}
}

unique_ptr<LocalOvr>
ActiveOvr::newTx()
{
	ver_t verRead = m_verBase;
	page_id_t pgidStartPage = m_pgidStartPage;
	for(LocalOvr* e = m_lovrVerifiedTip; e; e = e->m_prev)
	{
		if(! e->isMerged()) continue;

		verRead = e->m_verWrite;
		pgidStartPage = e->m_pgidStartPage;
		break;
	}
	return unique_ptr<LocalOvr>(new LocalOvr(m_hashOvrs, verRead, pgidStartPage));
}

void
ActiveOvr::terminate()
{
	// 1. put terminator lovr on tip to prevent further commit
	LocalOvr* terminator = new LocalOvr(nullptr, 0, 0);
	terminator->setTerminator();

	LocalOvr* currtip;
	do
	{
		currtip = m_lovrVerifiedTip;
		terminator->m_prev = currtip;
		__sync_synchronize(); // terminator->m_prev must be set BEFORE tip ptr CAS swing below
	}
	while(! __sync_bool_compare_and_swap(&m_lovrVerifiedTip, currtip, terminator));

	// 2. merge upto current tip
	mergeUpto(currtip);
}

void
ActiveOvr::merge(LocalOvr* lovr)
{
	// FIXME: really implement the concurrent merge
	if(! __sync_bool_compare_and_swap(&lovr->m_mergeOngoing, false, true))
	{
		MUTEXPROF_START("wait merge");
		// wait until merge complete
		while(! lovr->isMerged())
		{
			PTNK_MEMBARRIER_COMPILER;
		}
		MUTEXPROF_END;

		return;
	}

	const ver_t verWrite = lovr->m_verWrite;

	// int off = rand();
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		int roti = i; //(i + off) % TPIO_NHASH;

		// find the last entry of local ovrs and fill e->ver fields
		OvrEntry* laste = NULL;
		for(OvrEntry* e = lovr->m_hashOvrs[roti]; e && e->ver == LocalOvr::TAG_TXVER_LOCAL; e = e->prev)
		{
			laste = e;
			e->ver = verWrite;
		}

		// append the local ovrs list to the current list
		if(laste)
		{
			laste->prev = m_hashOvrs[roti];
			__sync_synchronize();

			m_hashOvrs[roti] = lovr->m_hashOvrs[roti];
		}
	}

	__sync_synchronize(); // m_merged must be set after actual merge finishes
	lovr->m_merged = true;
}

void
ActiveOvr::mergeUpto(LocalOvr* lovrTip)
{
	// step 1: create vector of un-merged txs 
	std::vector<LocalOvr*> lovrsUnmerged;
	for(LocalOvr* o = lovrTip; o && !o->isMerged(); o = o->m_prev)
	{
		if(o->m_bTerminator) continue;

		lovrsUnmerged.push_back(o);
	}

	// step 2: merge txs older one first
	typedef std::vector<LocalOvr*>::const_reverse_iterator it_t;
	for(it_t it = lovrsUnmerged.rbegin(); it != lovrsUnmerged.rend(); ++ it)
	{
		LocalOvr* o = *it;

		if(o->isMerged()) continue; // skip merged tx
		merge(o);
	}
}

ver_t
ActiveOvr::tryCommit(unique_ptr<LocalOvr>& plovr, ver_t verW)
{
	LocalOvr* lovr = plovr.get();

	// step 1: validate _lovr_ that it does not conflict with other txs and add _lovr_ to validated ovrs list
	{
		LocalOvr* lovrVerified = NULL;

		for(;;)
		{
			LocalOvr* lovrPrev = m_lovrVerifiedTip;

			// tx may not committed after terminator
			if(lovrPrev && lovrPrev->m_bTerminator) return PGID_INVALID;

			lovr->m_prev = lovrPrev;
			lovr->m_verWrite = (lovrPrev ? lovrPrev->m_verWrite : m_verBase) + 1;
			if(verW != TXID_INVALID)
			{
				PTNK_CHECK(lovr->m_verWrite <= verW)
				{
					std::cout << "lovr->m_verWrite: " << lovr->m_verWrite << std::endl;
					std::cout << "verW: " << verW << std::endl;
				}
				lovr->m_verWrite = verW;
			}
			__sync_synchronize(); // lovr->m_prev must be set BEFORE tip ptr CAS swing below

			// check conflict with txs committed after read snapshot
			for(LocalOvr* lovrBefore = lovrPrev; lovrBefore && lovrBefore != lovrVerified; lovrBefore = lovrBefore->m_prev)
			{
				if(lovr->m_verRead >= lovrBefore->m_verWrite)
				{
					// lovrBefore is tx before read snapshot. It does not conflict.

					break; // exit loop as older tx is also expected to be before read snapshot
				}

				if(lovr->checkConflict(lovrBefore))
				{
					// lovr conflicts with lovrBefore...

					// abort commit
					lovr->m_prev = nullptr;
					lovr->m_verWrite = 0;
					return TXID_INVALID;
				}
			}
			lovrVerified = lovrPrev;

			if(__sync_bool_compare_and_swap(&m_lovrVerifiedTip, lovrPrev, lovr))
			{
				// CAS succeed and _lovr_ has been successfully added to list of verified txs...

				// memo the tx ver
				verW = lovr->m_verWrite;
				
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
	mergeUpto(lovr);

	plovr.release();
	return verW;
}

ver_t
ActiveOvr::tryCommitRefresh(unique_ptr<LocalOvr>& plovr)
{
	LocalOvr* lovr = plovr.get();

	// step 1: remove ovr entries from _lovr_ which conflict with other txs and add _lovr_ to validated ovrs list
	ver_t verW = TXID_INVALID;
	{
		LocalOvr* lovrVerified = NULL;

		for(;;)
		{
			LocalOvr* lovrPrev = m_lovrVerifiedTip;

			// tx may not committed after terminator
			if(lovrPrev && lovrPrev->m_bTerminator) return PGID_INVALID;

			lovr->m_prev = lovrPrev;
			lovr->m_verWrite = (lovrPrev ? lovrPrev->m_verWrite : m_verBase) + 1;
			__sync_synchronize(); // lovr->m_prev must be set BEFORE tip ptr CAS swing below

			// check conflict with txs committed after read snapshot
			for(LocalOvr* lovrBefore = lovrPrev; lovrBefore && lovrBefore != lovrVerified; lovrBefore = lovrBefore->m_prev)
			{
				if(lovr->m_verRead >= lovrBefore->m_verWrite)
				{
					// lovrBefore is tx before read snapshot. It does not conflict.

					break; // exit loop as older tx is also expected to be before read snapshot
				}

				lovr->filterConflict(lovrBefore);
			}
			lovrVerified = lovrPrev;

			if(__sync_bool_compare_and_swap(&m_lovrVerifiedTip, lovrPrev, lovr))
			{
				// CAS succeed and _lovr_ has been successfully added to list of verified txs...

				// memo the tx ver
				verW = lovr->m_verWrite;
				
				break; // step 1 done!
			}
			else
			{
				// some other unverified tx has been added to the list...

				// RETRY!
			}
		}
	}

	PTNK_THROW_LOGIC_ERR("FIXME: not yet impl.");

	return verW;
}

void
ActiveOvr::dump(std::ostream& s) const
{
	s << "*** ActiveOvr dump" << std::endl;
	s << "verBase: " << m_verBase << std::endl;
	s << "last verified tip: " << std::endl;
	s << *m_lovrVerifiedTip << std::endl;
}

} // end of namespace ptnk
