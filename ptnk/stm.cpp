#include "stm.h"

namespace ptnk
{

std::ostream&
operator<<(std::ostream& s, const OvrEntry& e)
{
	s << "[orig " << e.pgidOrig << " -> ovr " << e.pgidOvr << " | ver: " << e.ver << " prev: " << (void*)e.prev << "]";
	return s;
}

LocalOvr::LocalOvr(OvrEntry* hashOvrs[], ver_t verRead)
:	m_verRead(verRead), m_verWrite(0),
	m_prev(NULL),
	m_mergeOngoing(false), m_merged(false)
{
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		m_hashOvrs[i] = hashOvrs[i];
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
	// add entry to m_pgidOrigs / Ovrs
	{
		m_pgidOrigs.push_back(pgidOrig);
		m_pgidOvrs.push_back(pgidOvr);
	}

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
	if(! other->m_bfOvrs.mayContain(m_bfOvrs)) return false;
	
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
	s << "* m_pgidOvrs dump" << std::endl;
	for(auto it = m_pgidOvrs.begin(); it != m_pgidOvrs.end(); ++ it)
	{
		s << *it << std::endl;
	}
}

ActiveOvr::ActiveOvr()
:	m_verRebase(1),
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
		if(lovr->m_merged)
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
	ver_t verRead = m_verRebase;
	for(LocalOvr* e = m_lovrVerifiedTip; e; e = e->m_prev)
	{
		if(! e->m_merged) continue;

		verRead = e->m_verWrite;
		break;
	}
	return unique_ptr<LocalOvr>(new LocalOvr(m_hashOvrs, verRead));
}

bool
ActiveOvr::tryCommit(LocalOvr* lovr)
{
	LocalOvr* lovr = plovr.get();

	// step 1: validate _lovr_ that it does not conflict with other txs and add _lovr_ to validated ovrs list
	{
		LocalOvr* lovrVerified = NULL;

		for(;;)
		{
			LocalOvr* lovrPrev = m_lovrVerifiedTip;
			lovr->m_prev = lovrPrev;
			lovr->m_verWrite = (lovrPrev ? lovrPrev->m_verWrite : m_verRebase) + 1;
			__sync_synchronize(); // lovr->m_prev must be set BEFORE tip ptr CAS swing below

			for(LocalOvr* lovrBefore = lovrPrev; lovrBefore && lovrBefore != lovrVerified; lovrBefore = lovrBefore->m_prev)
			{
				if(lovr->m_verRead >= lovrBefore->m_verWrite)
				{
					break;
				}

				if(lovr->checkConflict(lovrBefore))
				{
					// lovr conflicts with lovrBefore...

					// abort commit
					lovr->m_prev = nullptr;
					lovr->m_verWrite = 0;
					return false;
				}
			}
			lovrVerified = lovrPrev;

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

	plovr.release();
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

			m_hashOvrs[roti] = laste;
		}
	}

	__sync_synchronize(); // m_merged must be set after actual merge finishes
	lovr->m_merged = true;
}

void
ActiveOvr::dump(std::ostream& s) const
{
	s << "*** ActiveOvr dump" << std::endl;
	s << "verRebase: " << m_verRebase << std::endl;
	s << "last verified tip: " << std::endl;
	s << *m_lovrVerifiedTip << std::endl;
}

} // end of namespace ptnk
