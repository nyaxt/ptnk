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
std::ostream& operator<<(std::ostream& s, const OvrEntry& e);

std::ostream&
operator<<(std::ostream& s, const OvrEntry& e)
{
	s << "[orig " << e.pgidOrig << " -> ovr " << e.pgidOvr << " | ver: " << e.ver << " prev: " << (void*)e.prev << "]";
	return s;
}

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
	LocalOvr(OvrEntry* hashOvrs[], ver_t verRead);

	page_id_t searchOvr(page_id_t pgid);
	void addOvr(page_id_t pgidOrig, page_id_t pgidOvr);

	void dump(std::ostream& s) const;

private:
	enum { TAG_TXVER_LOCAL = 0 };

	Vpage_id_t m_pgidOrigs;
	Vpage_id_t m_pgidOvrs;

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

std::ostream& operator<<(std::ostream& s, const LocalOvr& o)
{ o.dump(s); return s; }

class ActiveOvr
{
public:
	ActiveOvr();

	LocalOvr* newTx();
	bool tryCommit(LocalOvr* lovr);
	bool tryCommit(std::unique_ptr<LocalOvr>& lovr)
	{
		bool ret = tryCommit(lovr.get());
		if(ret) lovr.release();
		return ret;
	}

	void dump(std::ostream& s) const;

private:
	void merge(LocalOvr* lovr);

	OvrEntry* m_hashOvrs[TPIO_NHASH];

	ver_t m_verRebase;

	//! latest verified tx
	/*!
	 *	tx in this linked-list are ensured that they do not conflict each other
	 */
	LocalOvr* m_lovrVerifiedTip;
};
std::ostream& operator<<(std::ostream& s, const ActiveOvr& o)
{ o.dump(s); return s; }

ActiveOvr::ActiveOvr()
:	m_lovrVerifiedTip(NULL),
	m_verRebase(1)
{
	for(int i = 0; i < TPIO_NHASH; ++ i)
	{
		m_hashOvrs[i] = NULL;
	}
}

LocalOvr*
ActiveOvr::newTx()
{
	ver_t verRead = m_verRebase;
	for(LocalOvr* e = m_lovrVerifiedTip; e; e = e->m_prev)
	{
		if(! e->m_merged) continue;

		verRead = e->m_verWrite;
		break;
	}
	return new LocalOvr(m_hashOvrs, verRead);
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

page_id_t
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
			return e->pgidOvr;
		}
	}

	return pgid; // no actie ovr pg found
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
			e->ver = lovr->m_verWrite;
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

} // end of namespace stm

} // end of namespace ptnk

#include <gtest/gtest.h>
#include <boost/thread.hpp>

using namespace ptnk;
using namespace ptnk::stm;

TEST(ptnk, stm_localovr)
{
	ActiveOvr ao;
	std::unique_ptr<LocalOvr> lo(ao.newTx());

	EXPECT_EQ((page_id_t)3, lo->searchOvr(3));

	lo->addOvr(1, 2);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(3));
	EXPECT_EQ((page_id_t)2, lo->searchOvr(2));
	EXPECT_EQ((page_id_t)2, lo->searchOvr(1));

	lo->addOvr(4, 5);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(3));
	EXPECT_EQ((page_id_t)2, lo->searchOvr(2));
	EXPECT_EQ((page_id_t)2, lo->searchOvr(1));
	EXPECT_EQ((page_id_t)5, lo->searchOvr(4));

	lo->addOvr(1, 3);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(1));
}

TEST(ptnk, stm_hash_collision)
{
	ActiveOvr ao;

	std::unique_ptr<LocalOvr> lo(ao.newTx());
	lo->addOvr(0, 1);
	lo->addOvr(0 + TPIO_NHASH, 2);
	lo->addOvr(0 + TPIO_NHASH*2, 3);
	
	EXPECT_EQ((page_id_t)1, lo->searchOvr(0));
	EXPECT_EQ((page_id_t)2, lo->searchOvr(0 + TPIO_NHASH));
	EXPECT_EQ((page_id_t)3, lo->searchOvr(0 + TPIO_NHASH*2));
}

TEST(ptnk, stm_basic)
{
	ActiveOvr ao;

	// basic commit op
	{
		std::unique_ptr<LocalOvr> lo(ao.newTx());

		lo->addOvr(1, 2);
		lo->addOvr(3, 4);

		EXPECT_TRUE(ao.tryCommit(lo));
		EXPECT_FALSE(lo.get());
	}

	{
		std::unique_ptr<LocalOvr> lo(ao.newTx());

		EXPECT_EQ((page_id_t)2, lo->searchOvr(1));
		EXPECT_EQ((page_id_t)4, lo->searchOvr(3));
		EXPECT_EQ((page_id_t)5, lo->searchOvr(5));

		lo->addOvr(5, 6);
		lo->addOvr(1, 8);

		EXPECT_EQ((page_id_t)8, lo->searchOvr(1));
		EXPECT_EQ((page_id_t)4, lo->searchOvr(3));
		EXPECT_EQ((page_id_t)6, lo->searchOvr(5));

		{
			std::unique_ptr<LocalOvr> lo2(ao.newTx());
			
			// un-committed transactions have no effect
			EXPECT_EQ((page_id_t)2, lo2->searchOvr(1));
			EXPECT_EQ((page_id_t)4, lo2->searchOvr(3));
			EXPECT_EQ((page_id_t)5, lo2->searchOvr(5));
		}

		EXPECT_TRUE(ao.tryCommit(lo));
		EXPECT_FALSE(lo.get());
	}

	// conflicting tx should fail
	{
		std::unique_ptr<LocalOvr> a(ao.newTx());
		std::unique_ptr<LocalOvr> b(ao.newTx());

		a->addOvr(10, 11);
		EXPECT_EQ((page_id_t)11, a->searchOvr(10));
		EXPECT_EQ((page_id_t)10, b->searchOvr(10));

		b->addOvr(10, 12);
		
		EXPECT_TRUE(ao.tryCommit(a));
		EXPECT_FALSE(a.get());
		EXPECT_FALSE(ao.tryCommit(b));
		EXPECT_TRUE(b.get());
	}
}

TEST(ptnk, stm_multithread)
{
	ActiveOvr ao;

	boost::thread_group tg;

	const int NUM_THREADS = 8;
	const int NUM_TX = 100000;
	for(int ith = 0; ith < NUM_THREADS; ++ ith)
	{
		tg.create_thread([&ao,ith]() {
			for(int i = 0; i < NUM_TX; ++ i)
			{
				std::unique_ptr<LocalOvr> t(ao.newTx());
				t->addOvr(i+NUM_TX*ith, i);
				
				EXPECT_TRUE(ao.tryCommit(t));
			}
		});
	}
	tg.join_all();

	std::cout << ao;

	if(false) // takes very long time
	{
		std::unique_ptr<LocalOvr> t(ao.newTx());
		
		for(int ith = 0; ith < NUM_THREADS; ++ ith)
		{
			for(int i = 0; i < NUM_TX; ++ i)
			{
				EXPECT_EQ((page_id_t)(i), t->searchOvr(i+NUM_TX*ith));
			}
		}
	}
}

TEST(ptnk, stm_multithread_w_conflict)
{
	ActiveOvr ao;

	boost::thread_group tg;

	const int NUM_TX = 100000;
	int committed_tx = -2;
	tg.create_thread([&]() {
		for(int i = 0; i < NUM_TX; ++ i)
		{
			// good tx	
			std::unique_ptr<LocalOvr> t(ao.newTx());
			t->addOvr(i, i + 100);
			
			EXPECT_TRUE(ao.tryCommit(t));

			committed_tx = i;
			__sync_synchronize(); // force write committed_tx
		}
		committed_tx = -1; // break loop in the other thread
	});

	tg.create_thread([&]() {
		for(int i = 0; i < NUM_TX; ++ i)
		{
			// bad tx
			// - begin tx by creating new snapshot
			std::unique_ptr<LocalOvr> t(ao.newTx());
			
			// - wait till a tx is committed
			__sync_synchronize(); // make sure we read latest committed_tx below
			int x = committed_tx;
			if(x == -1) break;
			while(x == committed_tx)
			{
				asm volatile("" : : : "memory");
			}

			// - do a conflicting change
			int tgt = x+1;
			t->addOvr(tgt, tgt + 200);
			
			// - this tx should fail to commit
			EXPECT_FALSE(ao.tryCommit(t));
		}
	});

	tg.join_all();

	std::cout << ao;
}
