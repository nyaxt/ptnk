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

	#if 1
	ver_t verWOther = other->verWrite();
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		for(OvrEntry* eL = m_hashOvrs[i]; eL && eL->ver == TAG_TXVER_LOCAL; eL = eL->prev)
		{
			page_id_t pgidL = eL->pgidOrig;
			if(! other->m_bfOvrs.mayContain(pgidL))
			{
				// a bloomfilter ensures that other->m_hashOvrs[i] list would not conflict w/ eL
				continue;
			}

			for(OvrEntry* eO = other->m_hashOvrs[i]; eO && (eO->ver == TAG_TXVER_LOCAL || eO->ver == verWOther); eO = eO->prev)
			{
				if(eO->pgidOrig == pgidL)
				{
					// detect conflict
					return true;
				}
			}
		}
	}
	#else
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
	#endif

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
	
	ver_t verWOther = other->verWrite();
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		for(OvrEntry* eL = m_hashOvrs[i]; eL && eL->ver == TAG_TXVER_LOCAL;)
		{
			page_id_t pgidL = eL->pgidOrig;
			if(! other->m_bfOvrs.mayContain(pgidL))
			{
				// a bloomfilter ensures that other->m_hashOvrs[i] list would not conflict w/ eL
				goto NEXT;	
			}

			for(OvrEntry* eO = other->m_hashOvrs[i]; eO && (eO->ver == TAG_TXVER_LOCAL || eO->ver == verWOther); eO = eO->prev)
			{
				if(eO->pgidOrig == pgidL)
				{
					// detect conflict

					// remove eL from the list
					OvrEntry* prev = eL->prev;
					delete eL;
					eL = prev;

					continue;
				}
			}

			NEXT:
			eL = eL->prev;
		}
	}
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
		PTNK_MEMBARRIER_HW; // terminator->m_prev must be set BEFORE tip ptr CAS swing below // FIXME FIXME shouldn't this be compiler memb?
	}
	while(! PTNK_CAS(&m_lovrVerifiedTip, currtip, terminator));

	// 2. merge upto current tip
	mergeUpto(currtip);
}

void
ActiveOvr::merge(LocalOvr* lovr)
{
	// FIXME: really implement the concurrent merge
	if(! PTNK_CAS(&lovr->m_mergeOngoing, false, true))
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

		// concat new OvrEntries (identified by e->ver == LocalOvr::TAG_TXVER_LOCAL)
		// to aovr::m_hashOvrs[i] 
		//
		// [x] : OvrEntry w/ ver _x_
		// <-  : OvrEntry::prev ptr
		//
		// BEFORE:
		//
		// m_hashOvrs[i] ss
		// @ start of tx     laste      lovr->m_hashOvrs[i]
		//   /----------------[L]<-[L]<-[L]
		//  v
		// [5]<-[6]<-[6]<-[7]<=aovr::m_hashOvrs[i] 
		//
		//     \------v-----/
		//       OvrEntries committed after ss
		// 
		// AFTER:
		//
		//                    [8]<-[8]<-[8]<=aovr::m_hashOvrs[i] 
		//                   /
		//                  v
		// [5]<-[6]<-[6]<-[7]
		//
		//                    \-----v-----/
		//                     these OvrEntries are now owned by aovr

		// find _laste_, the last entry of local ovrs and fill e->ver fields
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
			PTNK_MEMBARRIER_HW; // laste->prev must be set before tail upd

			m_hashOvrs[roti] = lovr->m_hashOvrs[roti];
		}
	}

	PTNK_MEMBARRIER_HW; // m_merged must be set after actual merge finishes
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
ActiveOvr::tryCommit(unique_ptr<LocalOvr>& plovr, commit_flags_t flags, ver_t verW)
{
	LocalOvr* lovr = plovr.get();

	// FIXME: optimize for case COMMIT_REPLAY (no conflict check needed)

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
			PTNK_MEMBARRIER_HW; // lovr->m_prev must be set BEFORE tip ptr CAS swing below

			// check conflict with txs committed after read snapshot
			for(LocalOvr* lovrBefore = lovrPrev; lovrBefore && lovrBefore != lovrVerified; lovrBefore = lovrBefore->m_prev)
			{
				if(lovr->m_verRead >= lovrBefore->m_verWrite)
				{
					// lovrBefore is tx before read snapshot. It does not conflict.

					break; // exit loop as older tx is also expected to be before read snapshot
				}

				if(PTNK_UNLIKELY(flags == COMMIT_REFRESH))
				{
					lovr->filterConflict(lovrBefore);
				}
				else // flags != COMMIT_REFRESH
				{
					if(lovr->checkConflict(lovrBefore))
					{
						// lovr conflicts with lovrBefore...

						// abort commit
						lovr->m_prev = nullptr;
						lovr->m_verWrite = 0;
						return TXID_INVALID;
					}
				}
			}
			lovrVerified = lovrPrev;

			if(PTNK_CAS(&m_lovrVerifiedTip, lovrPrev, lovr))
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

void
ActiveOvr::dump(std::ostream& s) const
{
	s << "*** ActiveOvr dump" << std::endl;
	s << "verBase: " << m_verBase << std::endl;
	s << "last verified tip: " << std::endl;
	s << *m_lovrVerifiedTip << std::endl;
}

} // end of namespace ptnk
