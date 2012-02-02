#include "ptnk/stm.h"

#include <iostream>
#include <thread>
#include <gtest/gtest.h>

using namespace ptnk;

typedef unique_ptr<std::thread> Pthread;

TEST(ptnk, stm_localovr)
{
	ActiveOvr ao;
	std::unique_ptr<LocalOvr> lo(ao.newTx());

	EXPECT_EQ((page_id_t)3, lo->searchOvr(3).first);

	lo->addOvr(1, 2);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(3).first);
	EXPECT_EQ((page_id_t)2, lo->searchOvr(2).first);
	EXPECT_EQ((page_id_t)2, lo->searchOvr(1).first);

	lo->addOvr(4, 5);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(3).first);
	EXPECT_EQ((page_id_t)2, lo->searchOvr(2).first);
	EXPECT_EQ((page_id_t)2, lo->searchOvr(1).first);
	EXPECT_EQ((page_id_t)5, lo->searchOvr(4).first);

	lo->addOvr(1, 3);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(1).first);
}

TEST(ptnk, stm_hash_collision)
{
	ActiveOvr ao;

	std::unique_ptr<LocalOvr> lo(ao.newTx());
	lo->addOvr(0, 1);
	lo->addOvr(0 + TPIO_NHASH, 2);
	lo->addOvr(0 + TPIO_NHASH*2, 3);
	
	EXPECT_EQ((page_id_t)1, lo->searchOvr(0).first);
	EXPECT_EQ((page_id_t)2, lo->searchOvr(0 + TPIO_NHASH).first);
	EXPECT_EQ((page_id_t)3, lo->searchOvr(0 + TPIO_NHASH*2).first);
}

TEST(ptnk, stm_basic)
{
	ActiveOvr ao;

	// basic commit op
	{
		std::unique_ptr<LocalOvr> lo(ao.newTx());

		lo->addOvr(1, 2);
		lo->addOvr(3, 4);

		ao.tryCommit(lo);
		EXPECT_FALSE(lo.get());
	}

	{
		std::unique_ptr<LocalOvr> lo(ao.newTx());

		EXPECT_EQ(make_pair((page_id_t)2, OVR_GLOBAL), lo->searchOvr(1));
		EXPECT_EQ(make_pair((page_id_t)4, OVR_GLOBAL), lo->searchOvr(3));
		EXPECT_EQ(make_pair((page_id_t)5, OVR_NONE),   lo->searchOvr(5));

		lo->addOvr(5, 6);
		lo->addOvr(1, 8);

		EXPECT_EQ(make_pair((page_id_t)8, OVR_LOCAL),  lo->searchOvr(1));
		EXPECT_EQ(make_pair((page_id_t)4, OVR_GLOBAL), lo->searchOvr(3));
		EXPECT_EQ(make_pair((page_id_t)6, OVR_LOCAL),  lo->searchOvr(5));

		{
			std::unique_ptr<LocalOvr> lo2(ao.newTx());
			
			// uncommitted transactions have no effect
			EXPECT_EQ(make_pair((page_id_t)2, OVR_GLOBAL), lo2->searchOvr(1));
			EXPECT_EQ(make_pair((page_id_t)4, OVR_GLOBAL), lo2->searchOvr(3));
			EXPECT_EQ(make_pair((page_id_t)5, OVR_NONE),   lo2->searchOvr(5));
		}

		ao.tryCommit(lo);
		EXPECT_FALSE(lo.get());
	}

	// conflicting tx should fail
	{
		std::unique_ptr<LocalOvr> a(ao.newTx());
		std::unique_ptr<LocalOvr> b(ao.newTx());

		a->addOvr(10, 11);
		EXPECT_EQ(make_pair((page_id_t)11, OVR_LOCAL), a->searchOvr(10));
		EXPECT_EQ(make_pair((page_id_t)10, OVR_NONE),  b->searchOvr(10));

		b->addOvr(10, 12);
		
		ao.tryCommit(a);
		EXPECT_FALSE(a.get());
		ao.tryCommit(b);
		EXPECT_TRUE(b.get());
	}
}

TEST(ptnk, stm_hash_collision_ci)
{
	ActiveOvr ao;

	{
		std::unique_ptr<LocalOvr> lo(ao.newTx());
		lo->addOvr(0, 1);
		lo->addOvr(0 + TPIO_NHASH, 2);
		lo->addOvr(0 + TPIO_NHASH*2, 3);
		
		ao.tryCommit(lo);
		EXPECT_FALSE(lo.get());
	}

	{
		std::unique_ptr<LocalOvr> lo(ao.newTx());
		EXPECT_EQ((page_id_t)1, lo->searchOvr(0).first);
		EXPECT_EQ((page_id_t)2, lo->searchOvr(0 + TPIO_NHASH).first);
		EXPECT_EQ((page_id_t)3, lo->searchOvr(0 + TPIO_NHASH*2).first);
	}
}

TEST(ptnk, stm_multithread)
{
	ActiveOvr ao;

	std::vector<Pthread> tg;

	const int NUM_THREADS = 8;
	const int NUM_TX = 100000;
	for(int ith = 0; ith < NUM_THREADS; ++ ith)
	{
		tg.push_back(Pthread(new std::thread([&ao,ith]() {
			for(int i = 0; i < NUM_TX; ++ i)
			{
				std::unique_ptr<LocalOvr> t(ao.newTx());
				t->addOvr(i+NUM_TX*ith, i);
				
				ao.tryCommit(t);
				EXPECT_FALSE(t.get());
			}
		})));
	}
	for(auto& t: tg) t->join();

	std::cout << ao;

	if(false) // takes very long time
	{
		std::unique_ptr<LocalOvr> t(ao.newTx());
		
		for(int ith = 0; ith < NUM_THREADS; ++ ith)
		{
			for(int i = 0; i < NUM_TX; ++ i)
			{
				EXPECT_EQ((page_id_t)(i), t->searchOvr(i+NUM_TX*ith).first);
			}
		}
	}
}

TEST(ptnk, stm_multithread_w_conflict)
{
	ActiveOvr ao;

	std::vector<Pthread> tg;

	const int NUM_TX = 1000000;
	volatile int committed_tx = -2;
	tg.push_back(Pthread(new std::thread([&]() {
		for(int i = 0; i < NUM_TX; ++ i)
		{
			// good tx	
			std::unique_ptr<LocalOvr> t(ao.newTx());
			t->addOvr(i, i + 100);

			ao.tryCommit(t);
			EXPECT_FALSE(t.get());

			committed_tx = i;
			PTNK_MEMBARRIER_HW; // force write committed_tx
		}
		committed_tx = -1; // break loop in the other thread
	})));

	tg.push_back(Pthread(new std::thread([&]() {
		// wait until tx progress
		while(committed_tx == -2)
		{
			PTNK_MEMBARRIER_COMPILER;
		}

		for(;;)
		{
			// bad tx
			// - begin tx by creating new snapshot
			std::unique_ptr<LocalOvr> t(ao.newTx());
			
			// - wait till a tx is committed
			PTNK_MEMBARRIER_HW; // make sure we read latest committed_tx below
			int x = committed_tx;
			while(x >= committed_tx)
			{
				if(committed_tx == -1) return;
				PTNK_MEMBARRIER_COMPILER;
			}

			// - do a conflicting change
			int tgt = x+1;
			bool alreadyCi = t->searchOvr(tgt).second != OVR_NONE;
			t->addOvr(tgt, tgt + 200);

			ao.tryCommit(t);
			if(! alreadyCi)
			{
				// - this tx should fail to commit
				EXPECT_TRUE(t.get()) << "bad tx success: " << tgt;
			}
			else
			{
				// - could not create conflicting tx
				EXPECT_FALSE(t.get()) << "bad tx fail: " << tgt;
			}
		}
	})));

	for(auto& t: tg) t->join();

	std::cout << ao;
}
