#include "ptnk/stm.h"

#include <iostream>
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

	const int NUM_TX = 1000000;
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
		// wait until tx progress
		while(committed_tx == -2)
		{
			asm volatile("" : : : "memory");
		}

		for(;;)
		{
			// bad tx
			// - begin tx by creating new snapshot
			std::unique_ptr<LocalOvr> t(ao.newTx());
			
			// - wait till a tx is committed
			__sync_synchronize(); // make sure we read latest committed_tx below
			int x = committed_tx;
			if(x == -1) break;
			while(x >= committed_tx)
			{
				asm volatile("" : : : "memory");
			}

			// - do a conflicting change
			int tgt = x+1;
			bool alreadyCi = t->searchOvr(tgt) != (page_id_t)tgt;
			t->addOvr(tgt, tgt + 200);
			
			if(! alreadyCi)
			{
				// - this tx should fail to commit
				EXPECT_FALSE(ao.tryCommit(t)) << "bad tx success: " << tgt;
			}
			else
			{
				// - could not create conflicting tx
				EXPECT_TRUE(ao.tryCommit(t)) << "bad tx fail: " << tgt;
			}
		}
	});

	tg.join_all();

	std::cout << ao;
}
