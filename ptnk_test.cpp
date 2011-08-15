#include "ptnk.h"
#include "ptnk/compmap.h"
#include "ptnk/partitionedpageio.h"
#include "ptnk/hash.h"
#include "ptnk/common.h"
#include "ptnk/types.h"
#include "ptnk/exceptions.h"
#include "ptnk/buffer.h"
#include "ptnk/pageiomem.h"
#include "ptnk/btree.h"
#include "ptnk/btree_int.h"
#include "ptnk/overview.h"
#include "ptnk/tpio.h"

#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include <dirent.h>
#include <sys/types.h>

#include <boost/tuple/tuple.hpp>

#include <gtest/gtest.h>
using namespace ptnk;

void
t_mktmpdir(const char* path)
{
	int ret = ::mkdir(path, 00755);
	if(ret < 0)
	{
		ASSERT_EQ(EEXIST, errno);

		// erase all existing files on the dir
		{
			DIR* dp;
			ASSERT_TRUE((dp = ::opendir(path)));

			struct dirent* e;
			while((e = ::readdir(dp)))
			{
				if(e->d_name[0] == '.') continue;

				std::string strFilePath(path);
				strFilePath.append("/");
				strFilePath.append(e->d_name);

				::unlink(strFilePath.c_str());
			}
		}
	}
}

void
t_makedummyfile(const char* path)
{
	int fd = ::open(path, O_RDWR | O_CREAT, 00644);
	ASSERT_TRUE(fd > 0);
	::write(fd, "dummy", 6);
	::close(fd);
}

const char* g_test_strs[10] = { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine" };

struct comp_val
{
	int* ary;
	int y;

	int operator()(int i)
	{
		return ary[i] - y;
	}
};

TEST(ptnk, test_idx_lower_bound)
{
	int ary[] = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};

	{
		comp_val c = {ary, 30};
		EXPECT_EQ(3, idx_lower_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 50};
		EXPECT_EQ(5, idx_lower_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 0};
		EXPECT_EQ(0, idx_lower_bound(0, 10, c));
	}

	{
		comp_val c = {ary, -1};
		EXPECT_EQ(0, idx_lower_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 90};
		EXPECT_EQ(9, idx_lower_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 100};
		EXPECT_EQ(10, idx_lower_bound(0, 10, c));
	}

	for(int i = 0; i <= 10; ++ i)
	{
		comp_val c = {ary, i * 10 - 5};
		// std::cout << c.y << " -> " << i << std::endl;
		EXPECT_EQ(i, idx_lower_bound(0, 10, c)) << "bin search failed for " << c.y;
	}

	int ary2[] = {1, 1, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7};

	{
		comp_val c = {ary2, 1};	
		EXPECT_EQ(0, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 2};	
		EXPECT_EQ(3, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 3};
		EXPECT_EQ(5, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 4};
		EXPECT_EQ(8, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 5};
		EXPECT_EQ(9, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 6};
		EXPECT_EQ(10, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 0};
		EXPECT_EQ(0, idx_lower_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 10};
		EXPECT_EQ(13, idx_lower_bound(0, 13, c));
	}
}

TEST(ptnk, test_idx_upper_bound)
{
	int ary[] = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};

	{
		comp_val c = {ary, 30};
		EXPECT_EQ(4, idx_upper_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 50};
		EXPECT_EQ(6, idx_upper_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 0};
		EXPECT_EQ(1, idx_upper_bound(0, 10, c));
	}

	{
		comp_val c = {ary, -1};
		EXPECT_EQ(0, idx_upper_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 90};
		EXPECT_EQ(10, idx_upper_bound(0, 10, c));
	}

	{
		comp_val c = {ary, 100};
		EXPECT_EQ(10, idx_upper_bound(0, 10, c));
	}

	for(int i = 0; i <= 10; ++ i)
	{
		comp_val c = {ary, i * 10 - 5};
		// std::cout << c.y << " -> " << i << std::endl;
		EXPECT_EQ(i, idx_upper_bound(0, 10, c)) << "bin search failed for " << c.y;
	}

	int ary2[] = {1, 1, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7};

	{
		comp_val c = {ary2, 1};	
		EXPECT_EQ(3, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 2};	
		EXPECT_EQ(5, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 3};
		EXPECT_EQ(8, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 4};
		EXPECT_EQ(9, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 5};
		EXPECT_EQ(10, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 6};
		EXPECT_EQ(12, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 0};
		EXPECT_EQ(0, idx_upper_bound(0, 13, c));
	}

	{
		comp_val c = {ary2, 10};
		EXPECT_EQ(13, idx_upper_bound(0, 13, c));
	}
}

TEST(ptnk, ptnk_memcmp)
{
	{
		uint8_t x[] = {0x04, 0x03, 0x02, 0x01};
		uint8_t y[] = {0x01, 0x02, 0x03, 0x04};
		EXPECT_EQ(0, ptnk_memcmp(x, x, 4));
		EXPECT_EQ(0, ptnk_memcmp(y, y, 4));

		EXPECT_LT(0, ptnk_memcmp(x, y, 4));
		EXPECT_GT(0, ptnk_memcmp(y, x, 4));
	}
}

TEST(ptnk, buffer)
{
	Buffer buf(32);
	EXPECT_FALSE(buf.isValid());
	EXPECT_EQ(-1, buf.valsize());
	// EXPECT_EQ(32U, buf.ressize());

	buf = cstr2ref("asdffdsa");
	EXPECT_TRUE(buf.isValid());
	EXPECT_EQ(8, buf.valsize());
	// EXPECT_EQ(32U, buf.ressize());
	buf.makeNullTerm();
	EXPECT_STREQ("asdffdsa", buf.get());
	
	Buffer copy(128);
	copy = buf;
	EXPECT_TRUE(copy.isValid());
	EXPECT_EQ(8, copy.valsize());
	// EXPECT_EQ(128U, copy.ressize());
	copy.makeNullTerm();
	EXPECT_STREQ("asdffdsa", copy.get());

	// assert that src buf has not changed
	EXPECT_TRUE(buf.isValid());
	EXPECT_EQ(8, buf.valsize());
	// EXPECT_EQ(32U, buf.ressize());
	buf.makeNullTerm();
	EXPECT_STREQ("asdffdsa", buf.get());
}

TEST(ptnk, buffercref_null)
{
	BufferCRef valid("asdf", 5);
	BufferCRef empty("", 0);
	BufferCRef null = BufferCRef::NULL_VAL;
	BufferCRef invalid;

	EXPECT_TRUE(valid.isValid());
	EXPECT_FALSE(valid.isNull());
	EXPECT_FALSE(valid.empty());

	EXPECT_TRUE(empty.isValid());
	EXPECT_FALSE(empty.isNull());
	EXPECT_TRUE(empty.empty());

	EXPECT_TRUE(null.isValid());
	EXPECT_TRUE(null.isNull());
	EXPECT_TRUE(null.empty());

	EXPECT_FALSE(invalid.isValid());
	EXPECT_FALSE(invalid.isNull());
	EXPECT_TRUE(invalid.empty());

	EXPECT_EQ(0, bufcmp(valid, valid));
	EXPECT_EQ(0, bufcmp(empty, empty));
	EXPECT_EQ(0, bufcmp(null, null));
	EXPECT_EQ(0, bufcmp(invalid, invalid));

	EXPECT_GT(0, bufcmp(empty, valid));
	EXPECT_GT(0, bufcmp(null, empty));
	EXPECT_LT(0, bufcmp(valid, empty));
	EXPECT_LT(0, bufcmp(empty, null));
}

TEST(ptnk, buffer_null)
{
	Buffer buf;
	EXPECT_FALSE(buf.isValid());

	buf.setValsize(bufcpy(buf.wref(), BufferCRef::NULL_VAL));
	EXPECT_TRUE(buf.isValid());
	EXPECT_TRUE(buf.isNull());

	buf.setValsize(bufcpy(buf.wref(), cstr2ref("asdffdsa")));
	EXPECT_TRUE(buf.isValid());
	EXPECT_FALSE(buf.isNull());

	buf.setValsize(bufcpy(buf.wref(), BufferCRef::NULL_VAL));
	EXPECT_TRUE(buf.isValid());
	EXPECT_TRUE(buf.isNull());
}

TEST(ptnk, leaf_verybasic)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());
	bool bOvr = false;
	btree_split_t split;

	l.insert(cstr2ref("key_a"), cstr2ref("val_a"), &split, &bOvr, pio.get());
	EXPECT_FALSE(split.isValid());
	
	Buffer val;
	val.setValsize(l.get(cstr2ref("key_a"), val.wref())); val.makeNullTerm(); EXPECT_STREQ("val_a", val.get());

	l.insert(cstr2ref("key_b"), cstr2ref("val_b"), &split, &bOvr, pio.get());
	l.dump(NULL);

	val.setValsize(l.get(cstr2ref("key_a"), val.wref())); val.makeNullTerm(); EXPECT_STREQ("val_a", val.get());
	val.setValsize(l.get(cstr2ref("key_b"), val.wref())); val.makeNullTerm(); EXPECT_STREQ("val_b", val.get());
}

// #define VERBOSE_LEAF_TEST
TEST(ptnk, leaf_bulk)
{
	const int NUM_KVS = 300;

	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());
	// std::cout << "sizeof Page: " << sizeof(Page) << std::endl;
	// std::cout << "sizeof Leaf: " << sizeof(Leaf) << std::endl;
	// l.dump();

	int i;
	bool bOvr = false;
	btree_split_t split;
	for(i = 0; i < NUM_KVS; ++ i)
	{
		char buf[8];
		sprintf(buf, "%u", i);

		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);
		l.insert(key, cstr2ref(buf), &split, &bOvr, pio.get());
		if(! split.isValid())
		{
			//  l.dump();
			for(int j = 0; j <= i; ++ j)
			{
				char bufC[8];
				Buffer buf;
				sprintf(bufC, "%u", j);

				uint32_t kb2 = __builtin_bswap32(j); BufferCRef key2(&kb2, 4);
				ssize_t s = l.get(key2, buf.wref()); buf.setValsize(s);
				EXPECT_EQ((ssize_t)::strlen(bufC), s) << "inserted up to " << i << ": incorrect val length for " << j;
				buf.makeNullTerm();
				EXPECT_STREQ(bufC, buf.get()) << "incorrect val str for " << j;
			}
		}
		else
		{
			break;	
		}
	}

#ifdef VERBOSE_LEAF_TEST
	std::cout << "i: " << i << std::endl;
	l.dump(pio.get());
#endif
	if(! split.isValid())
	{
		std::cout << "NUM_KVS: " << NUM_KVS << " too small to cause split";
		return;
	}

	Leaf l2(pio->readPage(split.split[0].pgid));
#ifdef VERBOSE_LEAF_TEST
	std::cout << " ========================= " << std::endl;
	l2.dump(pio.get());
#endif

	// all inserted entries must be found in either leaf
	Buffer buf1, buf2;
	for(; i >= 0; --i)
	{
		char bufC[8];
		sprintf(bufC, "%u", i);

		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);
		ssize_t s1 = l.get(key, buf1.wref()); buf1.setValsize(s1);
		ssize_t s2 = l2.get(key, buf2.wref()); buf2.setValsize(s2);
		buf1.makeNullTerm(); buf2.makeNullTerm(); 

		EXPECT_TRUE(s1 > 0 || s2 > 0) << "no val found for " << i;
		EXPECT_FALSE(s1 > 0 && s2 > 0) << "duplicate val found for " << i;
		if(s1 > 0)
		{
			EXPECT_EQ((ssize_t)::strlen(bufC), s1) << "incorrect val length for " << i;
			EXPECT_STREQ(bufC, buf1.get()) << "incorrect val str for " << i;
		}
		else if(s2 > 0)
		{
			EXPECT_EQ((ssize_t)::strlen(bufC), s2) << "incorrect val length for " << i;
			EXPECT_STREQ(bufC, buf2.get()) << "incorrect val str for " << i;
		}
	}
}

TEST(ptnk, leaf_null)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	Leaf l(pio->newInitPage<Leaf>());
	bool bOvr = false;

	BufferCRef empty("", 0);
	Buffer tmp;

	btree_split_t split;
	l.insert(empty, cstr2ref("test"), &split, &bOvr, pio.get());
	EXPECT_FALSE(split.isValid());

	tmp.setValsize(l.get(empty, tmp.wref())); tmp.makeNullTerm();
	EXPECT_STREQ("test", tmp.get());

	l.insert(BufferCRef::NULL_VAL, cstr2ref("hoge"), &split, &bOvr, pio.get());
	EXPECT_FALSE(split.isValid());

	tmp.setValsize(l.get(BufferCRef::NULL_VAL, tmp.wref())); tmp.makeNullTerm();
	EXPECT_STREQ("hoge", tmp.get());
}

TEST(ptnk, leaf_bulk_blobkey)
{
	Buffer key(1024);
	for(int i = 0; i < 1024; ++ i)
	{
		key.get()[i] = static_cast<char>(i);
	}

	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	Leaf l(pio->newInitPage<Leaf>());

	int i;
	bool bOvr = false;
	btree_split_t split;
	Buffer bufVal;
	for(i = 0; i < 1024; ++ i)
	{
		char buf[8]; sprintf(buf, "%u", i);

		key.setValsize(i);
		
		l.insert(key.rref(), cstr2ref(buf), &split, &bOvr, pio.get());
		// printf("inserted %u====================\n", i); l.dump(NULL);
		if(! split.isValid())
		{
			// ensure that inserted keys can be retrieved
			for(int j = 0; j <= i; ++ j)
			{
				char buf[8]; sprintf(buf, "%u", j);

				key.setValsize(j);
				bufVal.setValsize(l.get(key.rref(), bufVal.wref()));
				EXPECT_EQ((ssize_t)::strlen(buf), bufVal.valsize()) << "inserted up to " << i << ": incorrect val length for " << j;
				bufVal.makeNullTerm();
				EXPECT_STREQ(buf, bufVal.get()) << "inserted up to " << i << ": incorrect val str for " << j;
			}
		}
		else
		{
			break;	
		}
	}

	// the leaf page should have split by now
	EXPECT_TRUE(split.isValid());
	
	Leaf l2(pio->readPage(split.split[0].pgid));

	// all inserted entries must be found in either leaf
	Buffer buf1, buf2;
	for(; i >= 0; --i)
	{
		key.setValsize(i);

		char bufC[8]; sprintf(bufC, "%u", i);

		ssize_t s1 = l.get(key.rref(), buf1.wref()); buf1.setValsize(s1);
		ssize_t s2 = l2.get(key.rref(), buf2.wref()); buf2.setValsize(s2);

		buf1.makeNullTerm(); buf2.makeNullTerm();

		EXPECT_TRUE(s1 > 0 || s2 > 0) << "no val found for " << i;
		EXPECT_FALSE(s1 > 0 && s2 > 0) << "duplicate val found for " << i;
		if(s1 > 0)
		{
			EXPECT_EQ((ssize_t)::strlen(bufC), s1) << "incorrect val length for " << i;
			EXPECT_STREQ(bufC, buf1.get()) << "incorrect val str for " << i;
		}
		else if(s2 > 0)
		{
			EXPECT_EQ((ssize_t)::strlen(bufC), s2) << "incorrect val length for " << i;
			EXPECT_STREQ(bufC, buf2.get()) << "incorrect val str for " << i;
		
		}
	}
}

TEST(ptnk, leaf_random)
{
	for(int c = 0; c < 20; ++ c)
	{
		const int NUM_KVS = 300;

		int ord[NUM_KVS];
		for(int i = 0; i < NUM_KVS; ++i)
		{
			ord[i] = i;	
		}
		for(int i = 0; i < NUM_KVS*3; ++ i)
		{
			int x = i % NUM_KVS;
			int y = ::rand() % NUM_KVS;
			std::swap(ord[x], ord[y]);
		}

		boost::scoped_ptr<PageIO> pio(new PageIOMem);

		Leaf l(pio->newInitPage<Leaf>());

		int i;
		bool bOvr = false;
		btree_split_t split;
		for(i = 0; i < NUM_KVS; ++ i)
		{
			int t = ord[i];
			char buf[8];
			sprintf(buf, "%u", t);

			l.insert(BufferCRef(&t, sizeof(int)), cstr2ref(buf), &split, &bOvr, pio.get());
			if(! split.isValid())
			{
				for(int j = 0; j <= i; ++ j)
				{
					int tj = ord[j];

					char bufC[8];
					Buffer buf;
					sprintf(bufC, "%u", tj);

					ssize_t s = l.get(BufferCRef(&tj, sizeof(int)), buf.wref()); buf.setValsize(s);
					buf.makeNullTerm();
					EXPECT_EQ((ssize_t)::strlen(bufC), s) << "incorrect val length for " << tj;
					EXPECT_STREQ(bufC, buf.get()) << "incorrect val str for " << tj;
				}
			}
			else
			{
				break;	
			}
		}

	#ifdef VERBOSE_LEAF_TEST
		l.dump(pio.get());
	#endif
		if(! split.isValid())
		{
			std::cout << "NUM_KVS: " << NUM_KVS << " too small to cause split";
			return;
		}

		Leaf l2(pio->readPage(split.split[0].pgid));
	#ifdef VERBOSE_LEAF_TEST
		std::cout << " ========================= " << std::endl;
		l2.dump(pio.get());
	#endif

		Buffer buf1, buf2;
		for(; i >= 0; --i)
		{
			int t = ord[i];

			char bufC[8];
			sprintf(bufC, "%u", t);

			ssize_t s1 = l.get(BufferCRef(&t, sizeof(int)), buf1.wref()); buf1.setValsize(s1);
			ssize_t s2 = l2.get(BufferCRef(&t, sizeof(int)), buf2.wref()); buf2.setValsize(s2);
			buf1.makeNullTerm(); buf2.makeNullTerm();

			EXPECT_TRUE(s1 > 0 || s2 > 0) << "no val found for " << t;
			if(s1 > 0)
			{
				EXPECT_EQ((ssize_t)::strlen(bufC), s1) << "incorrect val length for " << t;
				EXPECT_STREQ(bufC, buf1.get()) << "incorrect val str for " << t;
			}
			else if(s2 > 0)
			{
				EXPECT_EQ((ssize_t)::strlen(bufC), s2) << "incorrect val length for " << t;
				EXPECT_STREQ(bufC, buf2.get()) << "incorrect val str for " << t;
			}
		}
	}
}

TEST(ptnk, leaf_corrupt)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	Leaf l(pio->newInitPage<Leaf>());

	btree_split_t split;
	for(int i = 1014; i < 1197; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "initial_%d", i);

		l.insert(cstr2ref(key), cstr2ref(val), &split, NULL, pio.get());
	}

	Buffer k, v;
	for(int i = 1014; i < 1197; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "initial_%d", i);

		btree_cursor_t cur; cur.leaf = l;
		query_t q = {cstr2ref(key), MATCH_EXACT};
		l.query(&cur, q);

		l.cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur);
		EXPECT_TRUE(k.isValid());
		EXPECT_TRUE(v.isValid());
		k.makeNullTerm(); v.makeNullTerm();
		EXPECT_STREQ(key, k.get());
		EXPECT_STREQ(val, v.get());
	}

	// l.dump(NULL);
}

TEST(ptnk, leaf_dupkey_simple)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());

	btree_split_t split;
	bool bOvr = false;
	l.insert(cstr2ref("hoge"), cstr2ref("a"), &split, &bOvr, pio.get()); EXPECT_FALSE(split.isValid()); EXPECT_FALSE(bOvr);
	l.insert(cstr2ref("hoge"), cstr2ref("b"), &split, &bOvr, pio.get()); EXPECT_FALSE(split.isValid()); EXPECT_FALSE(bOvr);
	l.insert(cstr2ref("hoge"), cstr2ref("c"), &split, &bOvr, pio.get()); EXPECT_FALSE(split.isValid()); EXPECT_FALSE(bOvr);

	// l.dump(NULL);
}

TEST(ptnk, leaf_dupkey_split)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());

	btree_split_t split;
	bool bOvr = false;

	int i;
	const int COUNT = 120;
	for(i = 0; i < COUNT; ++ i)
	{
		l.insert(cstr2ref("keyAAAAA"), cstr2ref("valAAAAA"), &split, &bOvr, pio.get());
		EXPECT_FALSE(split.isValid()); EXPECT_FALSE(bOvr);
	}
	for(i = 0; i < COUNT; ++ i)
	{
		l.insert(cstr2ref("keyCCCCC"), cstr2ref("valCCCCC"), &split, &bOvr, pio.get());
		EXPECT_FALSE(split.isValid()); EXPECT_FALSE(bOvr);
	}
	for(i = 0; i < COUNT; ++ i)
	{
		l.insert(cstr2ref("keyBBBBB"), cstr2ref("valBBBBB"), &split, &bOvr, pio.get());
		EXPECT_FALSE(bOvr);

		if(split.isValid())
		{
			// split occurred!
			break;
		}
	}
	ASSERT_NE(COUNT, i) << "try increasing constant COUNT";

#if 0
	std::cout << "i " << i << std::endl;
	l.dump(NULL);
	std::cout << "pgidSplit" << std::endl;
	pio->readPage(split.pgidSplit).dump(NULL);
	for(unsigned int j = 0; j < split.numSplit; ++ j)
	{
		std::cout << "*** split : " << split.split[j].key << std::endl;
		pio->readPage(split.split[j].pgid).dump(NULL);
	}
#endif
	ASSERT_EQ(1, split.numSplit);

	btree_cursor_t cur;
	cur.leaf = l;
	cur.idx = 0;
	Buffer k, v;
	for(int j = 0; j < COUNT; ++ j)
	{
		l.cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur);
		EXPECT_TRUE(k.isValid());
		EXPECT_TRUE(v.isValid());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_STREQ("keyAAAAA", k.get());
		EXPECT_STREQ("valAAAAA", v.get());

		++ cur.idx;
	}
	for(int j = 0; j < i; ++ j)
	{
		l.cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur);
		EXPECT_TRUE(k.isValid());
		EXPECT_TRUE(v.isValid());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_STREQ("keyBBBBB", k.get());
		EXPECT_STREQ("valBBBBB", v.get());

		++ cur.idx;
	}

	cur.leaf = pio->readPage(split.split[0].pgid);
	cur.idx = 0;
	for(int j = 0; j < COUNT; ++ j)
	{
		Leaf(cur.leaf).cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur);
		EXPECT_TRUE(k.isValid());
		EXPECT_TRUE(v.isValid());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_STREQ("keyCCCCC", k.get());
		EXPECT_STREQ("valCCCCC", v.get());

		++ cur.idx;
	}
}

TEST(ptnk, leaf_update_nosplit)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	Leaf l(pio->newInitPage<Leaf>());

	int i;
	bool bOvr; btree_split_t split;

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);
		char buf[8]; sprintf(buf, "%u", i);

		l.insert(key, cstr2ref(buf), &split, &bOvr, pio.get());

		EXPECT_FALSE(split.isValid());
	}
	l.insert(cstr2ref("null_key"), BufferCRef::NULL_VAL, &split, &bOvr, pio.get());

	Buffer v;
	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_TRUE(v.isValid());
		EXPECT_FALSE(v.isNull());
		EXPECT_STREQ(buf, v.get());
	}
	v.setValsize(l.get(cstr2ref("null_key"), v.wref()));
	EXPECT_TRUE(v.isNull());

	// update samesize
	{
		uint32_t kb = __builtin_bswap32(15); BufferCRef key(&kb, 4);
		l.update(key, cstr2ref("xx"), &split, &bOvr, pio.get());

		EXPECT_FALSE(split.isValid());
	}

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_TRUE(v.isValid());
		EXPECT_FALSE(v.isNull());

		if(i != 15)
		{
			EXPECT_STREQ(buf, v.get());
		}
		else
		{
			EXPECT_STREQ("xx", v.get());
		}
	}
	v.setValsize(l.get(cstr2ref("null_key"), v.wref()));
	EXPECT_TRUE(v.isNull());
	
	// update null 
	{
		uint32_t kb = __builtin_bswap32(15); BufferCRef key(&kb, 4);
		l.update(key, BufferCRef::NULL_VAL, &split, &bOvr, pio.get());

		EXPECT_FALSE(split.isValid());
	}

	// l.dump(NULL);

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		EXPECT_TRUE(v.isValid()) << "value invalid for key " << key;

		if(i != 15)
		{
			char buf[8]; sprintf(buf, "%u", i);
			EXPECT_FALSE(v.isNull());
			EXPECT_STREQ(buf, v.get());
		}
		else
		{
			EXPECT_TRUE(v.isNull());
		}
	}
	v.setValsize(l.get(cstr2ref("null_key"), v.wref()));
	EXPECT_TRUE(v.isNull());

	// update to larger val 
	{
		uint32_t kb = __builtin_bswap32(15); BufferCRef key(&kb, 4);
		l.update(key, cstr2ref("largervalue!!!"), &split, &bOvr, pio.get());

		EXPECT_FALSE(split.isValid());
	}
	// l.dump(NULL);

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_TRUE(v.isValid());
		EXPECT_FALSE(v.isNull());

		if(i != 15)
		{
			EXPECT_STREQ(buf, v.get());
		}
		else
		{
			EXPECT_STREQ("largervalue!!!", v.get());
		}
	}
	v.setValsize(l.get(cstr2ref("null_key"), v.wref()));
	EXPECT_TRUE(v.isNull());
}

TEST(ptnk, leaf_del)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	Leaf l(pio->newInitPage<Leaf>());

	int i;
	bool bOvr; btree_split_t split;

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);
		char buf[8]; sprintf(buf, "%u", i);

		l.insert(key, cstr2ref(buf), &split, &bOvr, pio.get());

		EXPECT_FALSE(split.isValid());
	}
	l.insert(BufferCRef::NULL_VAL, cstr2ref("null_key"), &split, &bOvr, pio.get());
	EXPECT_FALSE(split.isValid());

	Buffer v;
	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_TRUE(v.isValid());
		EXPECT_FALSE(v.isNull());
		EXPECT_STREQ(buf, v.get());
	}
	v.setValsize(l.get(BufferCRef::NULL_VAL, v.wref()));
	v.makeNullTerm();
	EXPECT_STREQ("null_key", v.get());

	// del key
	{
		btree_cursor_t c; c.leaf = l; c.idx = 16;
		EXPECT_TRUE(l.cursorDelete(&c, &bOvr, pio.get()));
	}

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		char buf[8]; sprintf(buf, "%u", i);

		if(i != 15)
		{
			EXPECT_TRUE(v.isValid());
			EXPECT_FALSE(v.isNull());
			EXPECT_STREQ(buf, v.get());
		}
		else
		{
			EXPECT_FALSE(v.isValid());
		}
	}
	v.setValsize(l.get(BufferCRef::NULL_VAL, v.wref()));
	v.makeNullTerm();
	EXPECT_STREQ("null_key", v.get());
}

TEST(ptnk, leaf_del2)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	Leaf l(pio->newInitPage<Leaf>());

	int i;
	bool bOvr; btree_split_t split;

	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);
		char buf[8]; sprintf(buf, "%u", i);

		l.insert(key, cstr2ref(buf), &split, &bOvr, pio.get());

		EXPECT_FALSE(split.isValid());
	}

	Buffer v;
	for(i = 0; i < 30; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

		v.setValsize(l.get(key, v.wref()));
		v.makeNullTerm();
		
		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_TRUE(v.isValid());
		EXPECT_FALSE(v.isNull());
		EXPECT_STREQ(buf, v.get());
	}

	for(int j = 29; j >= 1; -- j)
	{
		// del key
		{
			btree_cursor_t c; c.leaf = l; c.idx = j;
			EXPECT_TRUE(l.cursorDelete(&c, &bOvr, pio.get()));
		}

		for(i = 0; i < 30; ++ i)
		{
			uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);

			v.setValsize(l.get(key, v.wref()));
			v.makeNullTerm();
			
			char buf[8]; sprintf(buf, "%u", i);

			if(i < j)
			{
				EXPECT_TRUE(v.isValid());
				EXPECT_FALSE(v.isNull());
				EXPECT_STREQ(buf, v.get());
			}
			else
			{
				EXPECT_FALSE(v.isValid());	
			}
		}
	}

	// del last key
	{
		btree_cursor_t c; c.leaf = l; c.idx = 0;
		EXPECT_FALSE(l.cursorDelete(&c, &bOvr, pio.get()));
	}
}

TEST(ptnk, leaf_query)
{
	const int NUM_KVS = 300;

	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());
	// l.dump();

	int i;
	bool bOvr = false;
	btree_split_t split;
	for(i = 0; i < NUM_KVS; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i * 10); BufferCRef key(&kb, 4);

		char buf[8];
		sprintf(buf, "%u", i);

		l.insert(key, cstr2ref(buf), &split, &bOvr, pio.get());
		if(split.isValid()) break;	
	}

	static const uint32_t u300 = __builtin_bswap32(300); BufferCRef k300(&u300, 4);

	// exact match
	query_t q;
	uint32_t kb;
	q.type = MATCH_EXACT;
	q.key = BufferCRef(&kb, 4);
	{
		kb = __builtin_bswap32(300);

		btree_cursor_t cur; cur.leaf = l;
		Buffer key, value;

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));
	}

	// match or prev
	q.type = MATCH_OR_PREV;
	{
		kb = __builtin_bswap32(300);

		btree_cursor_t cur; cur.leaf = l;
		Buffer key, value;

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref())) << "key != 300 but " << (uint32_t)__builtin_bswap32(*(uint32_t*)key.get());
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));

		kb = __builtin_bswap32(301);

		l.query(&cur, q);
		ASSERT_TRUE(cur.isValid());
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref())) << "key != 300 but " << (uint32_t)__builtin_bswap32(*(uint32_t*)key.get());
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));
	}

	// match or next
	q.type = MATCH_OR_NEXT;
	{
		kb = __builtin_bswap32(300);

		btree_cursor_t cur; cur.leaf = l;
		Buffer key, value;

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref())) << "key != 300 but " << (uint32_t)__builtin_bswap32(*(uint32_t*)key.get());
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));

		kb = __builtin_bswap32(298);

		l.query(&cur, q);

		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref())) << "key != 300 but " << (uint32_t)__builtin_bswap32(*(uint32_t*)key.get());
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));
	}

	// before
	q.type = BEFORE;
	{
		kb = __builtin_bswap32(302);

		btree_cursor_t cur; cur.leaf = l;
		Buffer key, value;

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref())) << "key != 300 but " << (uint32_t)__builtin_bswap32(*(uint32_t*)key.get());
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));
	}

	// after
	q.type = AFTER;
	{
		kb = __builtin_bswap32(291);

		btree_cursor_t cur; cur.leaf = l;
		Buffer key, value;

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(k300, key.rref())) << "key != 300 but " << (uint32_t)__builtin_bswap32(*(uint32_t*)key.get());
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("30"), value.rref()));
	}
}

TEST(ptnk, leaf_query_torture)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());

	bool bOvr = false;
	btree_split_t split;
	l.insert(cstr2ref("1"), cstr2ref("val_1"), &split, &bOvr, pio.get()); EXPECT_FALSE(bOvr);
	l.insert(cstr2ref("2"), cstr2ref("val_2"), &split, &bOvr, pio.get()); EXPECT_FALSE(bOvr);
	l.insert(cstr2ref("3"), cstr2ref("val_3"), &split, &bOvr, pio.get()); EXPECT_FALSE(bOvr);
	l.insert(cstr2ref("4"), cstr2ref("val_4"), &split, &bOvr, pio.get()); EXPECT_FALSE(bOvr);

	btree_cursor_t cur; cur.leaf = l;
	Buffer key, value;

	query_t q;

	// exact match
	{
		q.type = MATCH_EXACT;
		q.key = cstr2ref("1");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("1"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_1"), value.rref()));
	}
	{
		q.type = MATCH_EXACT;
		q.key = cstr2ref("4");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("4"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_4"), value.rref()));
	}
	{
		q.type = MATCH_EXACT;
		q.key = cstr2ref("0");

		l.query(&cur, q);
		EXPECT_FALSE(cur.isValid());
	}
	{
		q.type = MATCH_EXACT;
		q.key = cstr2ref("5");

		l.query(&cur, q);
		EXPECT_FALSE(cur.isValid());
	}

	// match or prev
	{
		q.type = MATCH_OR_PREV;
		q.key = cstr2ref("1");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("1"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_1"), value.rref()));
	}
	{
		q.type = MATCH_OR_PREV;
		q.key = cstr2ref("4");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("4"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_4"), value.rref()));
	}
	{
		q.type = MATCH_OR_PREV;
		q.key = cstr2ref("0");

		l.query(&cur, q);
		EXPECT_TRUE(cur.isValid());

		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);
		EXPECT_FALSE(key.isValid());
		EXPECT_FALSE(value.isValid());
	}
	{
		q.type = MATCH_OR_PREV;
		q.key = cstr2ref("5");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("4"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_4"), value.rref()));
	}
	
	// match or next
	{
		q.type = MATCH_OR_NEXT;
		q.key = cstr2ref("1");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("1"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_1"), value.rref()));
	}
	{
		q.type = MATCH_OR_NEXT;
		q.key = cstr2ref("4");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("4"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_4"), value.rref()));
	}
	{
		q.type = MATCH_OR_NEXT;
		q.key = cstr2ref("0");

		l.query(&cur, q);
		EXPECT_TRUE(cur.isValid());

		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);
		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("1"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_1"), value.rref()));
	}
	{
		q.type = MATCH_OR_NEXT;
		q.key = cstr2ref("5");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_FALSE(key.isValid());
		EXPECT_FALSE(value.isValid());
	}

	// before
	{
		q.type = BEFORE;
		q.key = cstr2ref("1");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_FALSE(key.isValid());
		EXPECT_FALSE(value.isValid());
	}
	{
		q.type = BEFORE;
		q.key = cstr2ref("4");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("3"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_3"), value.rref()));
	}
	{
		q.type = BEFORE;
		q.key = cstr2ref("0");

		l.query(&cur, q);
		EXPECT_TRUE(cur.isValid());

		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);
		EXPECT_FALSE(key.isValid());
		EXPECT_FALSE(value.isValid());
	}
	{
		q.type = BEFORE;
		q.key = cstr2ref("5");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("4"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_4"), value.rref()));
	}

	// after 
	{
		q.type = AFTER;
		q.key = cstr2ref("1");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("2"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_2"), value.rref()));
	}
	{
		q.type = AFTER;
		q.key = cstr2ref("4");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_FALSE(key.isValid());
		EXPECT_FALSE(value.isValid());
	}
	{
		q.type = AFTER;
		q.key = cstr2ref("0");

		l.query(&cur, q);
		EXPECT_TRUE(cur.isValid());

		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);
		EXPECT_TRUE(key.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("1"), key.rref()));
		EXPECT_TRUE(value.isValid());
		EXPECT_TRUE(bufeq(cstr2ref("val_1"), value.rref()));
	}
	{
		q.type = AFTER;
		q.key = cstr2ref("5");

		l.query(&cur, q);
		l.cursorGet(key.wref(), key.pvalsize(), value.wref(), value.pvalsize(), cur);

		EXPECT_FALSE(key.isValid());
		EXPECT_FALSE(value.isValid());
	}
}

TEST(ptnk, leaf_cursor_put_basic)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Leaf l(pio->newInitPage<Leaf>());

	bool bOvr = false;
	btree_split_t split;
	l.insert(cstr2ref("key"), cstr2ref("initial_value"), &split, &bOvr, pio.get());
	EXPECT_FALSE(bOvr); EXPECT_FALSE(split.isValid());

	Buffer k, v;
	btree_cursor_t cur; cur.leaf = l;

	query_t q = {cstr2ref("key"), MATCH_EXACT};
	l.query(&cur, q);

	l.cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur);
	EXPECT_TRUE(k.isValid());
	EXPECT_TRUE(v.isValid());
	k.makeNullTerm(); v.makeNullTerm();
	EXPECT_STREQ("key", k.get());
	EXPECT_STREQ("initial_value", v.get());

	l.cursorPut(&cur, cstr2ref("hogefuga"), &split, &bOvr, pio.get());
	EXPECT_FALSE(bOvr); EXPECT_FALSE(split.isValid());

	l.cursorGet(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur);
	EXPECT_TRUE(k.isValid());
	EXPECT_TRUE(v.isValid());
	k.makeNullTerm(); v.makeNullTerm();
	EXPECT_STREQ("key", k.get());
	EXPECT_STREQ("hogefuga", v.get());
}

// #define DUMP_NODE_SPLIT
TEST(ptnk, node_bulk)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Node n(pio->newInitPage<Node>());
	n.initBody(0);

	const int COUNT = 2000;

	bool bOvr = false;
	btree_split_t split;
	int i;
	for(i = 0; i < COUNT; ++ i)
	{
		split.reset();
		split.pgidSplit = i;
		{
			uint32_t kb = __builtin_bswap32(split.pgidSplit+1);
			split.addSplit(BufferCRef(&kb, 4), i+1);
		}

		n.handleChildSplit(&split, &bOvr, pio.get());
		if(! split.isValid())
		{
#ifdef DUMP_NODE_SPLIT
			n.dump(NULL);
#endif
			for(int j = 0; j < i; ++ j)
			{
				query_t q;
				{
					uint32_t kb = __builtin_bswap32(j);
					q.key = BufferCRef(&kb, 4);
				}
				q.type = MATCH_EXACT;

				page_id_t r = n.query(q);
				EXPECT_EQ((unsigned)j, r) << i << "th itr failed to lu " << j << std::endl;
			}
		}
		else
		{
			break;	
		}
	}

	EXPECT_TRUE(split.isValid());

	Node n2(pio->readPage(split.split[0].pgid));

#ifdef DUMP_NODE_SPLIT
	std::cout << "after split" << std::endl;
	n.dump(NULL); n2.dump(NULL);
#endif

	for(; i >= 0; -- i)
	{
		query_t q;
		{
			uint32_t kb = __builtin_bswap32(i);
			q.key = BufferCRef(&kb, 4);
		}
		q.type = MATCH_EXACT;

		page_id_t r = n.query(q);
		page_id_t r2 = n2.query(q);

		EXPECT_TRUE(r == (page_id_t)i || r2 == (page_id_t)i) << "query for " << i << " failed\n r: " << r << " r2: " << r2; 
	}
}

TEST(ptnk, node_null)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Node n(pio->newInitPage<Node>());
	n.initBody(0);

	BufferCRef empty("", 0);
	bool bOvr = false;

	// insert null key
	{
		btree_split_t split;
		split.pgidSplit = 0;
		split.addSplit(BufferCRef::NULL_VAL, 1);
		
		n.handleChildSplit(&split, &bOvr, pio.get());
		EXPECT_FALSE(bOvr);
	}

	// insert empty key
	{
		btree_split_t split;
		split.pgidSplit = 1;
		split.addSplit(empty, 2);
		
		n.handleChildSplit(&split, &bOvr, pio.get());
		EXPECT_FALSE(bOvr);
	}

	{
		query_t q;
		q.type = MATCH_EXACT;

		q.key = BufferCRef::NULL_VAL;
		EXPECT_EQ(1U, n.query(q));

		q.key = empty;
		EXPECT_EQ(2U, n.query(q));
	}
}

void get_random_ord(int ord[], int size)
{
	for(int i = 0; i < size; ++ i)
	{
		ord[i] = i;	
	}
	for(int i = 0; i < size * 10; ++ i)
	{
		std::swap(
			ord[::rand() % size],
			ord[::rand() % size]
			);
	}
}
#define SETUP_ORD(size) \
	int ord[(size)]; get_random_ord(ord, (size));

TEST(ptnk, node_random)
{
	const int COUNT = 2000;
	for(int c = 0; c < 10; ++ c)
	{
		boost::scoped_ptr<PageIO> pio(new PageIOMem);

		SETUP_ORD(COUNT);

		Node n(pio->newInitPage<Node>());
		n.initBody(0);

		int i;
		bool bOvr = false;
		btree_split_t split;
		for(i = 0; i < COUNT; ++ i)
		{
			int t = ord[i]+1;
			uint32_t tb = __builtin_bswap32(t);

			split.reset();
			{
				query_t q;
				q.key = BufferCRef(&tb, 4);
				q.type = MATCH_EXACT;
				split.pgidSplit = n.query(q);
			}
			uint32_t kb = __builtin_bswap32(t);
			split.addSplit(BufferCRef(&kb, 4), t);

			n.handleChildSplit(&split, &bOvr, pio.get());
			if(! split.isValid())
			{
	#ifdef DUMP_NODE_SPLIT
				n.dump(NULL);
	#endif
				for(int j = 0; j < i; ++ j)
				{
					unsigned int t = ord[j]+1;
					uint32_t tb = __builtin_bswap32(t);

					query_t q;
					q.key = BufferCRef(&tb, 4);
					q.type = MATCH_EXACT;

					page_id_t r = n.query(q);
					EXPECT_EQ(t, r);
				}
			}
			else
			{
				break;	
			}
		}

		Node n2(pio->readPage(split.split[0].pgid));

	#ifdef DUMP_NODE_SPLIT
		n.dump(NULL); n2.dump(NULL);
	#endif

		for(; i >= 0; -- i)
		{
			int t = ord[i]+1;
			uint32_t tb = __builtin_bswap32(t);

			query_t q;
			q.key = BufferCRef(&tb, 4);
			q.type = MATCH_EXACT;

			page_id_t r = n.query(q);
			page_id_t r2 = n2.query(q);

			if(! (r == (page_id_t)t || r2 == (page_id_t)t))
			{
				std::cout << "r: " << r << " r2: " << r2 << std::endl;	
			}
			EXPECT_TRUE(r == (page_id_t)t || r2 == (page_id_t)t) << "query num " << i << " for " << t << " failed"; 
		}
		{
			unsigned int t = 0;
			uint32_t tb = __builtin_bswap32(t);

			query_t q;
			q.key = BufferCRef(&tb, 4);
			q.type = MATCH_EXACT;

			page_id_t r = n.query(q);
			EXPECT_EQ(t, r);
		}
	}
}

TEST(ptnk, node_del)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Node n(pio->newInitPage<Node>());
	n.initBody(0);

	bool bOvr;
	btree_split_t split;
	for(int i = 0; i < 5; ++ i)
	{
		split.reset();
		split.pgidSplit = i;
		uint8_t kb = i * 2 + 1;
		split.addSplit(BufferCRef(&kb, 1), i+1);

		n.handleChildSplit(&split, &bOvr, pio.get());
		EXPECT_FALSE(split.isValid());
	}

	for(int i = 0; i < 6; ++ i)
	{
		query_t q;
		uint8_t kb = i * 2;
		q.key = BufferCRef(&kb, 1);
		q.type = MATCH_EXACT;

		page_id_t r = n.query(q);
		EXPECT_EQ((page_id_t)i, r) << "query for " << i << " failed";
	}

#ifdef VERBOSE_NODE_DEL
	n.dump(NULL);
#endif

	// try delete child 3
	page_id_t next;
	EXPECT_TRUE(n.handleChildDel(&next, 3, &bOvr, pio.get()).isValid());
	EXPECT_EQ(4, next);

#ifdef VERBOSE_NODE_DEL
	n.dump(NULL);
#endif

	for(int i = 0; i < 6; ++ i)
	{
		query_t q;
		uint8_t kb = i * 2;
		q.key = BufferCRef(&kb, 1);
		q.type = MATCH_EXACT;

		page_id_t r = n.query(q);
		if(i != 3)
		{
			EXPECT_EQ((page_id_t)i, r) << "query for " << i << " failed";
		}
		else
		{
			EXPECT_EQ((page_id_t)2, r) << "query for " << i << " failed";
		}
	}
}

TEST(ptnk, node_del_first)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Node n(pio->newInitPage<Node>());
	n.initBody(0);

	bool bOvr;
	btree_split_t split;
	for(int i = 0; i < 5; ++ i)
	{
		split.reset();
		split.pgidSplit = i;
		uint8_t kb = i * 2 + 1;
		split.addSplit(BufferCRef(&kb, 1), i+1);

		n.handleChildSplit(&split, &bOvr, pio.get());
		EXPECT_FALSE(split.isValid());
	}

	for(int i = 0; i < 6; ++ i)
	{
		query_t q;
		uint8_t kb = i * 2;
		q.key = BufferCRef(&kb, 1);
		q.type = MATCH_EXACT;

		page_id_t r = n.query(q);
		EXPECT_EQ((page_id_t)i, r) << "query for " << i << " failed";
	}

#ifdef VERBOSE_NODE_DEL
	n.dump(NULL);
#endif

	// try delete first child 0
	page_id_t next;
	EXPECT_TRUE(n.handleChildDel(&next, 0, &bOvr, pio.get()).isValid());
	EXPECT_EQ(1, next);

#ifdef VERBOSE_NODE_DEL
	n.dump(NULL);
#endif

	for(int i = 0; i < 6; ++ i)
	{
		query_t q;
		uint8_t kb = i * 2;
		q.key = BufferCRef(&kb, 1);
		q.type = MATCH_EXACT;

		page_id_t r = n.query(q);
		if(i != 0)
		{
			EXPECT_EQ((page_id_t)i, r) << "query for " << i << " failed";
		}
		else
		{
			EXPECT_EQ((page_id_t)1, r) << "query for " << i << " failed";
		}
	}
}

TEST(ptnk, node_del_only)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Node n(pio->newInitPage<Node>());
	n.initBody(0);
	
	// try deleting the only entry left
	bool bOvr = false;
	page_id_t next;
	EXPECT_FALSE(n.handleChildDel(&next, 0, &bOvr, pio.get()).isValid());
	EXPECT_FALSE(bOvr);
	EXPECT_EQ(PGID_INVALID, next);
}

TEST(ptnk, node_query)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	Node n(pio->newInitPage<Node>());
	n.initBody(0);

	const int COUNT = 5;

	bool bOvr = false;
	btree_split_t split;
	for(int i = 0; i < COUNT; ++ i)
	{
		split.reset();
		split.pgidSplit = i;
		uint8_t kb = i * 2;
		split.addSplit(BufferCRef(&kb, 1), i+1);

		n.handleChildSplit(&split, &bOvr, pio.get());
		EXPECT_FALSE(split.isValid());
	}

	// n.dump(NULL);

	// exact match
	query_t q;
	uint8_t kb;
	q.type = MATCH_EXACT;
	q.key = BufferCRef(&kb, 1);
	{
		kb = 3;
		EXPECT_EQ(2U, n.query(q));

		kb = 4;
		EXPECT_EQ(3U, n.query(q));

		kb = 5;
		EXPECT_EQ(3U, n.query(q));
	}

	// match or prev
	q.type = MATCH_OR_PREV;
	{
		kb = 3;
		EXPECT_EQ(2U, n.query(q));

		kb = 4;
		EXPECT_EQ(3U, n.query(q));

		kb = 5;
		EXPECT_EQ(3U, n.query(q));
	} 

	// match or next
	q.type = MATCH_OR_NEXT;
	{
		kb = 3;
		EXPECT_EQ(2U, n.query(q));

		kb = 4;
		EXPECT_EQ(3U, n.query(q));

		kb = 5;
		EXPECT_EQ(3U, n.query(q));
	}

	// before
	q.type = BEFORE;
	{
		kb = 3;
		EXPECT_EQ(2U, n.query(q));

		kb = 4;
		EXPECT_EQ(2U, n.query(q));

		kb = 5;
		EXPECT_EQ(3U, n.query(q));
	}

	// after
	q.type = AFTER;
	{
		kb = 3;
		EXPECT_EQ(2U, n.query(q));

		kb = 4;
		EXPECT_EQ(3U, n.query(q));

		kb = 5;
		EXPECT_EQ(3U, n.query(q));
	}
}

TEST(ptnk, btree_basic)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	
	page_id_t idRoot = btree_init(pio.get());

	idRoot = btree_put(idRoot, cstr2ref("hoge"), cstr2ref("fuga"), PUT_INSERT, PGID_INVALID, pio.get());

	Buffer value;
	value.setValsize(btree_get(idRoot, cstr2ref("hoge"), value.wref(), pio.get()));
	value.makeNullTerm();
	EXPECT_STREQ("fuga", value.get());

	idRoot = btree_put(idRoot, cstr2ref("hoge"), cstr2ref("asdffdsa"), PUT_UPDATE, PGID_INVALID, pio.get());

	value.setValsize(btree_get(idRoot, cstr2ref("hoge"), value.wref(), pio.get()));
	value.makeNullTerm();
	EXPECT_STREQ("asdffdsa", value.get());

	idRoot = btree_del(idRoot, cstr2ref("hoge"), pio.get());

	value.setValsize(btree_get(idRoot, cstr2ref("hoge"), value.wref(), pio.get()));
	EXPECT_FALSE(value.isValid());
}

TEST(ptnk, btree_cursor_get_first)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	
	page_id_t idRoot = btree_init(pio.get());

	idRoot = btree_put(idRoot, cstr2ref("hoge"), cstr2ref("fuga"), PUT_INSERT, PGID_INVALID, pio.get());

	btree_cursor_wrap cur;
	btree_cursor_front(cur.get(), idRoot, pio.get());

	Buffer key, value;
	btree_cursor_get(
		key.wref(), key.pvalsize(),
		value.wref(), value.pvalsize(),
		cur.get(), pio.get());
	key.makeNullTerm(); value.makeNullTerm();

	EXPECT_STREQ("hoge", key.get());
	EXPECT_STREQ("fuga", value.get());
}

TEST(ptnk, btree_cursor_nextprev)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	
	page_id_t idRoot = btree_init(pio.get());

	const int NUM_KVS = 10000;

	for(int i = 0; i < NUM_KVS; ++ i)
	{
		uint32_t kb = __builtin_bswap32(i); BufferCRef key(&kb, 4);
		char buf[8]; sprintf(buf, "%u", i);
		idRoot = btree_put(idRoot, key, cstr2ref(buf), PUT_INSERT, PGID_INVALID, pio.get());
	}

	btree_cursor_wrap cur;
	btree_cursor_front(cur.get(), idRoot, pio.get());

	Buffer key, value;

	for(int i = 0; i < NUM_KVS; ++ i)
	{
		btree_cursor_get(
			key.wref(), key.pvalsize(),
			value.wref(), value.pvalsize(),
			cur.get(), pio.get());
		value.makeNullTerm();

		uint32_t kb = __builtin_bswap32(i);
		EXPECT_EQ(kb, *(uint32_t*)key.get());

		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_STREQ(buf, value.get());

		EXPECT_EQ(btree_cursor_next(cur.get(), pio.get()), i != NUM_KVS-1);
	}

	btree_cursor_back(cur.get(), idRoot, pio.get());
	for(int i = NUM_KVS-1; i >= 0; -- i)
	{
		btree_cursor_get(
			key.wref(), key.pvalsize(),
			value.wref(), value.pvalsize(),
			cur.get(), pio.get());
		value.makeNullTerm();

		uint32_t kb = __builtin_bswap32(i);
		EXPECT_EQ(kb, *(uint32_t*)key.get());

		char buf[8]; sprintf(buf, "%u", i);
		EXPECT_STREQ(buf, value.get());

		EXPECT_EQ(btree_cursor_prev(cur.get(), pio.get()), i != 0);
	}
}

TEST(ptnk, dupkey_tree_10k)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	page_id_t idRoot = btree_init(pio.get());

	BufferCRef dupkey = cstr2ref("dupkey");
	static const int DUPKEY_COUNT = 10000;
	for(int i = 0; i < DUPKEY_COUNT; ++ i)
	{
		char value[32]; sprintf(value, "dupvalue%d", i);
		idRoot = btree_put(idRoot, dupkey, cstr2ref(value), PUT_INSERT, PGID_INVALID, pio.get());
	}

	Buffer k, v;

	// front -> back scan
	btree_cursor_wrap cur;
	btree_cursor_front(cur.get(), idRoot, pio.get());
	for(int i = 0; i < DUPKEY_COUNT; ++ i)
	{
		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();
		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(k.get(), "dupkey");
		EXPECT_TRUE(v.isValid());
		EXPECT_TRUE(::strncmp(v.get(), "dupvalue", 8) == 0) << "value: " << v;

		ASSERT_EQ(i != DUPKEY_COUNT-1, btree_cursor_next(cur.get(), pio.get()));
	}

	// back -> front
	btree_cursor_back(cur.get(), idRoot, pio.get());
	for(int i = 0; i < DUPKEY_COUNT; ++ i)
	{
		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();
		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(k.get(), "dupkey");
		EXPECT_TRUE(v.isValid());
		EXPECT_TRUE(::strncmp(v.get(), "dupvalue", 8) == 0) << "value: " << v;

		ASSERT_EQ(i != DUPKEY_COUNT-1, btree_cursor_prev(cur.get(), pio.get())) << "i: " << i;
	}
}

TEST(ptnk, btree_many_dupkeys)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	
	page_id_t idRoot = btree_init(pio.get());
	
	BufferCRef dupkey = cstr2ref("dupkey");
	static const int DUPKEY_COUNT = 400;
	for(int i = 0; i < DUPKEY_COUNT; ++ i)
	{
		char value[32]; sprintf(value, "dupvalue%d", i);
		idRoot = btree_put(idRoot, dupkey, cstr2ref(value), PUT_INSERT, PGID_INVALID, pio.get());
	}
	idRoot = btree_put(idRoot, cstr2ref("cccccc"), cstr2ref("cval"), PUT_INSERT, PGID_INVALID, pio.get());
	idRoot = btree_put(idRoot, cstr2ref("eeeeee"), cstr2ref("eval"), PUT_INSERT, PGID_INVALID, pio.get());

	// pio->readPage(idRoot).dump(pio.get());

	Buffer k, v;
	v.setValsize(btree_get(idRoot, cstr2ref("cccccc"), v.wref(), pio.get()));
	v.makeNullTerm();
	EXPECT_TRUE(v.isValid());
	EXPECT_STREQ(v.get(), "cval");

	v.setValsize(btree_get(idRoot, cstr2ref("eeeeee"), v.wref(), pio.get()));
	v.makeNullTerm();
	EXPECT_TRUE(v.isValid());
	EXPECT_STREQ(v.get(), "eval");

	// - test front -> back scan by cursor
	btree_cursor_wrap cur;
	btree_cursor_front(cur.get(), idRoot, pio.get());

	{
		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();
		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(k.get(), "cccccc");
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(v.get(), "cval");

		btree_cursor_next(cur.get(), pio.get());
	}

	for(int i = 0; i < DUPKEY_COUNT; ++ i)
	{
		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();
		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(k.get(), "dupkey");
		EXPECT_TRUE(v.isValid());
		EXPECT_TRUE(::strncmp(v.get(), "dupvalue", 8) == 0) << "value: " << v;

		btree_cursor_next(cur.get(), pio.get());
	}

	{
		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();
		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(k.get(), "eeeeee");
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(v.get(), "eval");
	}

	// - test querying all dupkey entries by cursor
	{
		query_t q = {dupkey, MATCH_EXACT};
		btree_query(cur.get(), idRoot, q, pio.get());
	}

	int c = 0;
	for(;;)
	{
		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		if(! bufeq(k.rref(), dupkey)) break;

		++ c;	
		btree_cursor_next(cur.get(), pio.get());
	}

	EXPECT_EQ(DUPKEY_COUNT, c);
}

TEST(ptnk, btree_cursor_put)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	page_id_t idRoot = btree_init(pio.get());

	const int COUNT = 10000;

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "initial_%d", i);

		idRoot = btree_put(idRoot, cstr2ref(key), cstr2ref(val), PUT_INSERT, PGID_INVALID, pio.get());
	}

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "initial_%d", i);

		Buffer value;
		value.setValsize(btree_get(idRoot, cstr2ref(key), value.wref(), pio.get()));
		value.makeNullTerm();

		EXPECT_STREQ(val, value.get());
	}

	Buffer k, v;

	btree_cursor_wrap cur;
	btree_cursor_front(cur.get(), idRoot, pio.get());
	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "initial_%d", i);

		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(key, k.get());
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(val, v.get());

		sprintf(val, "modified_%d", i);
		idRoot = btree_cursor_put(cur.get(), cstr2ref(val), pio.get());

		ASSERT_EQ(i != COUNT-1, btree_cursor_next(cur.get(), pio.get()));
	}

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "modified_%d", i);

		Buffer value;
		value.setValsize(btree_get(idRoot, cstr2ref(key), value.wref(), pio.get()));
		value.makeNullTerm();

		EXPECT_STREQ(val, value.get());
	}
}

TEST(ptnk, OverviewPage)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	OverviewPage ovv(pio->newInitPage<OverviewPage>());
	
	bool bOvr = false;
	ovv.setTableRoot(cstr2ref("table_a"), 10, &bOvr, pio.get());
	ovv.setTableRoot(cstr2ref("table_b"), 20, &bOvr, pio.get());
	ovv.setTableRoot(cstr2ref("table_c"), 30, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(20, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(30, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.setTableRoot(cstr2ref("table_b"), 21, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(21, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(30, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.dropTable(cstr2ref("table_b"), &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(PGID_INVALID, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(30, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.setTableRoot(cstr2ref("table_b"), 20, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(20, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(30, ovv.getTableRoot(cstr2ref("table_c")));

	// default table is the first table on the db
	EXPECT_EQ(10, ovv.getDefaultTableRoot());
	ovv.setDefaultTableRoot(11, &bOvr, pio.get());
	EXPECT_EQ(11, ovv.getDefaultTableRoot());

	// ovv.dump(NULL);
}

TEST(ptnk, btree_cursor_del)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	page_id_t idRoot = btree_init(pio.get());

	const int COUNT = 1000;

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		idRoot = btree_put(idRoot, cstr2ref(key), cstr2ref(val), PUT_INSERT, PGID_INVALID, pio.get());
	}

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		Buffer value;
		value.setValsize(btree_get(idRoot, cstr2ref(key), value.wref(), pio.get()));
		value.makeNullTerm();

		EXPECT_STREQ(val, value.get());
	}

	Buffer k, v;

	btree_cursor_wrap cur;
	btree_cursor_front(cur.get(), idRoot, pio.get());
	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(key, k.get());
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(val, v.get());

		if(i % 2)
		{
			bool bCurValid;
			boost::tie(bCurValid, idRoot) = btree_cursor_del(cur.get(), pio.get());
			ASSERT_EQ(i != COUNT-1, bCurValid);
		}
		else
		{
			ASSERT_EQ(i != COUNT-1, btree_cursor_next(cur.get(), pio.get()));
		}
	}

	btree_cursor_front(cur.get(), idRoot, pio.get());
	for(int i = 0; i < COUNT; ++ i)
	{
		if(i % 2) continue;

		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(key, k.get());
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(val, v.get());

		ASSERT_EQ(i != COUNT-2, btree_cursor_next(cur.get(), pio.get()));
	}
}

TEST(ptnk, btree_cursor_del_wholeleaf)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	page_id_t idRoot = btree_init(pio.get());

	const int COUNT = 1000;

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		idRoot = btree_put(idRoot, cstr2ref(key), cstr2ref(val), PUT_INSERT, PGID_INVALID, pio.get());
	}

	for(int i = 0; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		Buffer value;
		value.setValsize(btree_get(idRoot, cstr2ref(key), value.wref(), pio.get()));
		value.makeNullTerm();

		EXPECT_STREQ(val, value.get());
	}

	// Page(pio->readPage(idRoot)).dump(pio.get());

	int iFirst, iLast;
	{
		Buffer strFirst;
		strFirst = Leaf(pio->readPage(2)).keyFirst();
		strFirst.makeNullTerm();
		
		iFirst = atoi(strFirst.get());
	}
	{
		Buffer strLast;
		strLast = Leaf(pio->readPage(3)).keyFirst();
		strLast.makeNullTerm();

		iLast = atoi(strLast.get()) - 1;
	}

	std::cout << "iFirst: " << iFirst << " iLast: " << iLast << std::endl;

	Buffer k, v;

	btree_cursor_wrap cur;
	{
		char key[8]; sprintf(key, "%d", iFirst);
		query_t q = {cstr2ref(key), MATCH_EXACT};
		btree_query(cur.get(), idRoot, q, pio.get());
	}
	int i;
	for(i = iFirst; i <= iLast; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_TRUE(k.isValid()) << "i: " << i;
		EXPECT_STREQ(key, k.get()) << "i: " << i;
		EXPECT_TRUE(v.isValid()) << "i: " << i;
		EXPECT_STREQ(val, v.get()) << "i: " << i;

		bool bCurValid;
		boost::tie(bCurValid, idRoot) = btree_cursor_del(cur.get(), pio.get());
		ASSERT_EQ(true, bCurValid) << "i: " << i;
	}
	for(; i < COUNT; ++ i)
	{
		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(key, k.get());
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(val, v.get());

		ASSERT_EQ(i != COUNT-1, btree_cursor_next(cur.get(), pio.get()));
	}

	btree_cursor_front(cur.get(), idRoot, pio.get());
	for(i = 0; i < COUNT; ++ i)
	{
		if(iFirst <= i && i <= iLast) continue;

		char key[8]; sprintf(key, "%d", i);
		char val[16]; sprintf(val, "val_%d", i);

		btree_cursor_get(k.wref(), k.pvalsize(), v.wref(), v.pvalsize(), cur.get(), pio.get());
		k.makeNullTerm(); v.makeNullTerm();

		EXPECT_TRUE(k.isValid());
		EXPECT_STREQ(key, k.get());
		EXPECT_TRUE(v.isValid());
		EXPECT_STREQ(val, v.get());

		ASSERT_EQ(i != COUNT-1, btree_cursor_next(cur.get(), pio.get()));
	}
}

TEST(ptnk, OverviewPage_cache)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	OverviewPage ovv(pio->newInitPage<OverviewPage>());
	
	bool bOvr = false;
	TableOffCache a(cstr2ref("table_a"));
	TableOffCache b(cstr2ref("table_b"));
	TableOffCache c(cstr2ref("table_c"));

	ovv.setTableRoot(&a, 10, &bOvr, pio.get());
	ovv.setTableRoot(&b, 20, &bOvr, pio.get());
	ovv.setTableRoot(&c, 30, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(&a));
	EXPECT_EQ(20, ovv.getTableRoot(&b));
	EXPECT_EQ(30, ovv.getTableRoot(&c));
	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(20, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(30, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.setTableRoot(&b, 21, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(&a));
	EXPECT_EQ(21, ovv.getTableRoot(&b));
	EXPECT_EQ(30, ovv.getTableRoot(&c));
	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(21, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(30, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.setTableRoot(cstr2ref("table_b"), 22, &bOvr, pio.get());
	ovv.setTableRoot(cstr2ref("table_c"), 31, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(&a));
	EXPECT_EQ(22, ovv.getTableRoot(&b));
	EXPECT_EQ(31, ovv.getTableRoot(&c));
	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(22, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(31, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.dropTable(cstr2ref("table_b"), &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(&a));
	EXPECT_EQ(PGID_INVALID, ovv.getTableRoot(&b));
	EXPECT_EQ(31, ovv.getTableRoot(&c));
	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(PGID_INVALID, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(31, ovv.getTableRoot(cstr2ref("table_c")));

	ovv.setTableRoot(&b, 23, &bOvr, pio.get());

	EXPECT_EQ(10, ovv.getTableRoot(&a));
	EXPECT_EQ(23, ovv.getTableRoot(&b));
	EXPECT_EQ(31, ovv.getTableRoot(&c));
	EXPECT_EQ(10, ovv.getTableRoot(cstr2ref("table_a")));
	EXPECT_EQ(23, ovv.getTableRoot(cstr2ref("table_b")));
	EXPECT_EQ(31, ovv.getTableRoot(cstr2ref("table_c")));

	// default table is the first table on the db
	EXPECT_EQ(10, ovv.getDefaultTableRoot());
	ovv.setDefaultTableRoot(11, &bOvr, pio.get());
	EXPECT_EQ(11, ovv.getDefaultTableRoot());

	// ovv.dump(NULL);
}

#define MY_ASSERT_NO_THROW(code) \
	ASSERT_NO_THROW({ \
	try{ \
		code; \
	} \
	catch(std::exception& e) { \
		std::cout << "exception caught: " << e.what() << std::endl; \
		throw; \
	} \
	});

TEST(ptnk, overridesv)
{
	OverridesV ovrs;

	EXPECT_EQ((page_id_t)3, ovrs.find(3));

	ovrs.add(1, 2);
	EXPECT_EQ((page_id_t)3, ovrs.find(3));
	EXPECT_EQ((page_id_t)2, ovrs.find(2));
	EXPECT_EQ((page_id_t)2, ovrs.find(1));

	ovrs.add(4, 5);
	EXPECT_EQ((page_id_t)3, ovrs.find(3));
	EXPECT_EQ((page_id_t)2, ovrs.find(2));
	EXPECT_EQ((page_id_t)2, ovrs.find(1));
	EXPECT_EQ((page_id_t)5, ovrs.find(4));

	ovrs.add(1, 3);
	EXPECT_EQ((page_id_t)3, ovrs.find(1));
	// EXPECT_EQ((page_id_t)3, ovrs.find(2)); // dousiyou
}

TEST(ptnk, OverridesCB_getSpaceLeft)
{
	OverridesCB ocb;

	OverridesCB::hint_t hint = ocb.getHint();
	EXPECT_EQ(OverridesCB::BUF_SIZE - 1, ocb.getSpaceLeft(1, hint));

	{
		OverridesV ovrs;
		ovrs.add(1, 2);
		ovrs.add(2, 3);
		ovrs.add(3, 4);
		
		EXPECT_TRUE(ocb.checkConflict(ovrs, 1, 1, hint));
		ocb.merge(ovrs, 1, 1, 2);
	}
	EXPECT_EQ(OverridesCB::BUF_SIZE - 1 - 3, ocb.getSpaceLeft(1, hint));
	
	hint = ocb.getHint();
	{
		OverridesV ovrs;
		for(int i = 0; i < OverridesCB::BUF_SIZE / 2; ++ i)
		{
			ovrs.add(i+3, i+4);	
		}

		EXPECT_TRUE(ocb.checkConflict(ovrs, 2, 2, hint));
		ocb.merge(ovrs, 2, 2, 3);
	}
	EXPECT_EQ(OverridesCB::BUF_SIZE - 1 - OverridesCB::BUF_SIZE/2, ocb.getSpaceLeft(1, hint));

	hint = ocb.getHint();
	{
		OverridesV ovrs;
		for(int i = 0; i < OverridesCB::BUF_SIZE / 2; ++ i)
		{
			ovrs.add(i+100000, i+100001);	
		}

		EXPECT_TRUE(ocb.checkConflict(ovrs, 3, 3, hint));
		ocb.merge(ovrs, 3, 3, 4);
	}
	EXPECT_EQ(-1, ocb.getSpaceLeft(1, hint));
	EXPECT_EQ(OverridesCB::BUF_SIZE - 1 - OverridesCB::BUF_SIZE/2, ocb.getSpaceLeft(4, hint));
}

TEST(ptnk, TPIO_basic)
{
	boost::shared_ptr<PageIO> pio(new PageIOMem);
	TPIO tpio(pio);

	page_id_t pgid;
	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
		
		DebugPage pg(tx1->newInitPage<DebugPage>());
		bool bOvr = false;
		pg.set('a', &bOvr, tx1.get());

		ASSERT_EQ('a', pg.get());
		ASSERT_FALSE(bOvr);
		
		DebugPage pgR(tx1->readPage(pgid = pg.pageId()));
		ASSERT_EQ(pg.get(), pgR.get());

		ASSERT_TRUE(tx1->tryCommit());
	}

	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
		boost::scoped_ptr<TPIOTxSession> tx2(tpio.newTransaction());
		
		DebugPage pg1(tx1->readPage(pgid));
		ASSERT_EQ('a', pg1.get());

		bool bOvr = false;
		pg1.set('b', &bOvr, tx1.get());
		ASSERT_TRUE(bOvr);

		ASSERT_EQ('a', pg1.get()) << "original page is changed";

		DebugPage pg2(tx2->readPage(pgid));
		ASSERT_EQ('a', pg2.get()) << "other's modification is seen"; 

		DebugPage pg3(tx1->readPage(pgid));
		ASSERT_EQ('b', pg3.get()) << "failed self modified read";

		pg2.set('c', &bOvr, tx2.get());
		ASSERT_TRUE(bOvr);

		DebugPage pg4(tx2->readPage(pgid));
		ASSERT_EQ('c', pg4.get()) << "failed self modified read";

		ASSERT_EQ('b', pg3.get()) << "other's modification is seen";

		ASSERT_TRUE(tx1->tryCommit());
		ASSERT_FALSE(tx2->tryCommit()) << "tx containing same page mod should not success";
	}
}

TEST(ptnk, TPIO_multiupdate)
{
	boost::shared_ptr<PageIO> pio(new PageIOMem);
	TPIO tpio(pio);

	page_id_t pgid;
	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
		
		DebugPage pg(tx1->newInitPage<DebugPage>());
		bool bOvr = false;
		pg.set('a', &bOvr, tx1.get());

		ASSERT_EQ('a', pg.get());
		ASSERT_FALSE(bOvr);
		
		DebugPage pgR(tx1->readPage(pgid = pg.pageId()));
		ASSERT_EQ(pg.get(), pgR.get());

		ASSERT_TRUE(tx1->tryCommit());
	}

	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
			
		DebugPage pg1(tx1->readPage(pgid));
		ASSERT_EQ('a', pg1.get());

		bool bOvr = false;
		pg1.set('b', &bOvr, tx1.get());
		ASSERT_TRUE(bOvr);

		ASSERT_EQ('a', pg1.get()) << "original page is changed";

		DebugPage pg2(tx1->readPage(pgid));
		ASSERT_EQ('b', pg2.get()) << "modification not found";

		// second mod will not create ovr
		bOvr = false;
		pg2.set('c', &bOvr, tx1.get());
		ASSERT_FALSE(bOvr);

		ASSERT_EQ('a', pg1.get()) << "original page is changed";
		ASSERT_EQ('c', pg2.get()) << "original page is not changed";

		DebugPage pg3(tx1->readPage(pgid));
		ASSERT_EQ('c', pg3.get()) << "modification not found";

		ASSERT_TRUE(tx1->tryCommit());
	}

	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
		DebugPage pg1(tx1->readPage(pgid));
		ASSERT_EQ('c', pg1.get()) << "mod not seen from other tx";
	}
}

TEST(ptnk, TPIO_commitfail)
{
	boost::shared_ptr<PageIO> pio(new PageIOMem);
	TPIO tpio(pio);

	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
		
		for(int i = 0; i < 10; ++ i)
		{
			DebugPage pg(tx1->newInitPage<DebugPage>());
			bool bOvr = false;
			pg.set('a', &bOvr, tx1.get());

			ASSERT_EQ('a', pg.get());
			ASSERT_FALSE(bOvr);

			ASSERT_EQ((page_id_t)i, pg.pageId());
		}

		ASSERT_TRUE(tx1->tryCommit());
	}

	for(int k = 0; k < 20; ++ k)
	{
		boost::scoped_ptr<TPIOTxSession> tx1(tpio.newTransaction());
		boost::scoped_ptr<TPIOTxSession> tx2(tpio.newTransaction());

		int x[3];
		for(int j = 0; j < 3; ++ j)
		{
		PICK_OTHER:
			x[j] = rand() % 10;
			for(int i = 0; i < j; ++ i)
			{
				if(x[j] == x[i]) goto PICK_OTHER;
			}
			DebugPage pgx(tx1->readPage(x[j]));

			bool bOvr = false;
			pgx.set('x', &bOvr, tx1.get());
			ASSERT_TRUE(bOvr);
		}

		bool shouldfail = false;
		int y[3];
		for(int j = 0; j < 3; ++ j)
		{
		PICK_OTHER2:
			y[j] = rand() % 10;
			for(int i = 0; i < j; ++ i)
			{
				if(y[j] == y[i]) goto PICK_OTHER2;
			}
			DebugPage pgy(tx2->readPage(y[j]));
			for(int i = 0; i < 3; ++ i)
			{
				if(y[j] == x[i]) shouldfail = true;
			}

			bool bOvr = false;
			pgy.set('y', &bOvr, tx2.get());
			ASSERT_TRUE(bOvr);
		}

		ASSERT_TRUE(tx1->tryCommit());
		ASSERT_EQ(!shouldfail, tx2->tryCommit());
	}
}

TEST(ptnk, tx_single_key_put_get)
{
	DB db;
	BufferCRef value_put("one", 4);

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		tx->put(cstr2ref("1"), value_put);

		{
			Buffer v;
			db.get(cstr2ref("1"), &v);
			// should not see not yet committed transaction
			EXPECT_FALSE(v.isValid());

			tx->get(cstr2ref("1"), &v);
			EXPECT_TRUE(v.isValid());
			EXPECT_EQ((ssize_t)value_put.size(), v.valsize());
			v.makeNullTerm();
			EXPECT_STREQ(value_put.get(), v.get());
		}
		EXPECT_TRUE(tx->tryCommit());
	}
	
	Buffer v(32);
	db.get(cstr2ref("1"), &v);
	EXPECT_EQ((ssize_t)value_put.size(), v.valsize());
	v.makeNullTerm();
	EXPECT_STREQ(value_put.get(), v.get());
}

TEST(ptnk, single_key_put_get)
{
	DB db;
	BufferCRef value_put = cstr2ref("one");

	db.put(cstr2ref("1"), value_put);
	
	Buffer value_get(32);
	db.get(cstr2ref("1"), &value_get);

	EXPECT_TRUE(value_get.isValid());
	EXPECT_EQ(value_put.size(), value_get.valsize());
	EXPECT_EQ(0, ::memcmp(value_put.get(), value_get.get(), value_put.size()));
}

// #define DUMP_TENKEYS
TEST(ptnk, ten_key_put_get)
{
	DB db;
	
	MY_ASSERT_NO_THROW({
		for(int i = 0; i < 10; ++ i)
		{
			db.put(BufferCRef(&i, 1), cstr2ref(g_test_strs[i]));

#ifdef DUMP_TENKEYS
#if 1
			printf("*** %u ********************\n", i);
			if(i == 4)
			{
				printf("hogehoge\n");
			}
			db.dump();
#else
			{
				char fn[255];
				sprintf(fn, "graphdump/%d.gv", i);
				FILE* fp = fopen(fn, "w");
				db.dumpGraph(fp);
				fclose(fp);
			}
#endif
#endif

		}
	});

	Buffer value_get;
	for(int i = 0; i < 10; ++ i)
	{
		db.get(BufferCRef(&i, 1), &value_get);
		ASSERT_TRUE(value_get.isValid());

		BufferCRef value_correct = cstr2ref(g_test_strs[i]);
		EXPECT_EQ((ssize_t)value_correct.size(), value_get.valsize()) << "incorrect value size for " << i;
		EXPECT_EQ(0, ::memcmp(value_correct.get(), value_get.get(), value_correct.size())) << "incorrect value for " << i;
		// std::cout << i << ": " << (char*)value_get.p << std::endl;
	}
}

TEST(ptnk, hundred_key_put_get)
{
	DB db;
	const int NUM_KEYS = 100;

	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char buf[8];
		sprintf(buf, "%u", i);
		db.put(BufferCRef(&i, 1), BufferCRef(buf, ::strlen(buf)+1));
	}

	{
		FILE* fp = fopen("graphdump/hundred.gv", "w");
		db.dumpGraph(fp);
		fclose(fp);
	}

	Buffer v(32);
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char bufCorrect[8];
		sprintf(bufCorrect, "%u", i);

		db.get(BufferCRef(&i, 1), &v);
		ASSERT_TRUE(v.isValid());
		EXPECT_EQ((ssize_t)::strlen(bufCorrect)+1, v.valsize()) << "incorrect val length for " << i;
		v.makeNullTerm();
		EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
	}
}

TEST(ptnk, random_keys_put_get)
{
	DB db;
	const int NUM_KEYS = 1000;

	int ord[NUM_KEYS];
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		ord[i] = i;
	}
	for(int i = 0; i < 10000; ++ i)
	{
		int x = i % NUM_KEYS;	
		int y = rand() % NUM_KEYS;
		std::swap(ord[x], ord[y]);
	}

	for(int j = 0; j < NUM_KEYS; ++ j)
	{
		int i = ord[j];

		char buf[8];
		sprintf(buf, "%u", i);

		db.put_k32u(i, BufferCRef(buf, ::strlen(buf)+1));
		// printf("put %u\n", i);

		for(int l = 0; l < j; ++ l)
		{
			int k = ord[l];
			char bufCorrect[8];
			sprintf(bufCorrect, "%u", k);

			Buffer v;
			db.get_k32u(k, &v);
			EXPECT_TRUE(v.isValid()) << "value for " << k << " not found after insert of " << i << " idx: " << j;
			if(v.isValid())
			{
				EXPECT_EQ((ssize_t)::strlen(bufCorrect)+1, v.valsize()) << "value for " << k << " invalid length";
				v.makeNullTerm();
				EXPECT_STREQ(bufCorrect, v.get()) << "value for " << k << " mismatch after insert of " << i << " idx: " << j;
			}
		}
	}

	if(0) {
		FILE* fp = fopen("graphdump/random.gv", "w");
		db.dumpGraph(fp);
		fclose(fp);
	}

	if(0)
	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		std::cout << "pgidstart: " << tx->pio()->pgidStartPage() << std::endl;;
	}

	Buffer v;
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char bufCorrect[8];
		sprintf(bufCorrect, "%u", i);

		db.get_k32u(i, &v);
		EXPECT_TRUE(v.isValid()) << "value for " << i << " not found";
		if(v.isValid())
		{
			EXPECT_EQ((ssize_t)::strlen(bufCorrect)+1, v.valsize()) << "value for " << i << " invalid length";
			v.makeNullTerm();
			EXPECT_STREQ(bufCorrect, v.get()) << "value for " << i << " mismatch";
		}
	}

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		tx->put_k32u(50, cstr2ref("asjdkfljfdsa"));
		// db.dumpStat();
		// tx->dumpStat();
		EXPECT_TRUE(tx->tryCommit());
	}
}

// #define VERBOSE_REBASE_TEST
TEST(ptnk, rebase_test)
{
	DB db;
	
	for(int i = 0; i < 9; ++ i)
	{
		db.put_k32u(i, cstr2ref(g_test_strs[i]));
	}
	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		tx->put_k32u(9, cstr2ref(g_test_strs[9]));
		// tx->dumpStat();
		EXPECT_TRUE(tx->tryCommit());
	}

	Buffer value_get;
	for(int i = 0; i < 10; ++ i)
	{
		db.get_k32u(i, &value_get);
		ASSERT_TRUE(value_get.isValid());

		BufferCRef value_correct = cstr2ref(g_test_strs[i]);
		EXPECT_EQ((ssize_t)value_correct.size(), value_get.valsize()) << "incorrect value size for " << i;
		EXPECT_EQ(0, ::memcmp(value_correct.get(), value_get.get(), value_correct.size())) << "incorrect value for " << i;
	}

	{
		FILE* fp = fopen("graphdump/before_rebase.gv", "w");
		db.dumpGraph(fp);
		fclose(fp);
	}

	db.rebase();
#ifdef VERBOSE_REBASE_TEST
	db.dumpAll();
#endif

	{
		FILE* fp = fopen("graphdump/after_rebase.gv", "w");
		db.dumpGraph(fp);
		fclose(fp);
	}


	for(int i = 0; i < 10; ++ i)
	{
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
			tx->get_k32u(i, &value_get);
			// tx->dumpStat();
			EXPECT_EQ(0U, tx->pio()->stat().nReadOvr) << "number of read ovr should be 0 after rebase";
		}
		ASSERT_TRUE(value_get.isValid()) << "value " << i << " is missing after rebase";

		BufferCRef value_correct = cstr2ref(g_test_strs[i]);
		EXPECT_EQ((ssize_t)value_correct.size(), value_get.valsize()) << "incorrect value size for " << i << " after rebase";
		EXPECT_EQ(0, ::memcmp(value_correct.get(), value_get.get(), value_correct.size())) << "incorrect value for " << i << " after rebase";
	}

	MY_ASSERT_NO_THROW({
		for(int i = 10; i < 20; ++i)
		{
			char buf[8];
			sprintf(buf, "%u", i);

			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
			tx->put_k32u(i, cstr2ref(buf));
			EXPECT_TRUE(tx->tryCommit());
		}
	});

	for(int i = 0; i < 10; ++ i)
	{
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
			tx->get_k32u(i, &value_get);
			// tx->dumpStat();
		}
		ASSERT_TRUE(value_get.isValid());

		BufferCRef value_correct = cstr2ref(g_test_strs[i]);
		EXPECT_EQ((ssize_t)value_correct.size(), value_get.valsize()) << "incorrect value size for " << i;
		EXPECT_EQ(0, ::memcmp(value_correct.get(), value_get.get(), value_correct.size())) << "incorrect value for " << i;
	}

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		for(int i = 10; i < 20; ++i)
		{
			char buf[8], buf2[8];
			sprintf(buf, "%u", i);

			size_t size = tx->get_k32u(i, BufferRef(buf2, sizeof(buf2)));
			EXPECT_EQ(::strlen(buf), size) << "incorrect value size for " << i;
			EXPECT_EQ(0, ::memcmp(buf, buf2, size)) << "incorrect value for " << i;
		}
	}

	// db.dump();
	// db.dumpAll();
	db.rebase();
#ifdef VERBOSE_REBASE_TEST
	db.dumpAll();
#endif

	for(int i = 0; i < 10; ++ i)
	{
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
			tx->get_k32u(i, &value_get);
			// tx->dumpStat();
		}
		ASSERT_TRUE(value_get.isValid());

		BufferCRef value_correct = cstr2ref(g_test_strs[i]);
		EXPECT_EQ((ssize_t)value_correct.size(), value_get.valsize()) << "incorrect value size for " << i;
		EXPECT_EQ(0, ::memcmp(value_correct.get(), value_get.get(), value_correct.size())) << "incorrect value for " << i;
	}

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		for(int i = 10; i < 20; ++i)
		{
			char buf[8], buf2[8];
			sprintf(buf, "%u", i);

			size_t size = tx->get_k32u(i, BufferRef(buf2, sizeof(buf2)));
			EXPECT_EQ(::strlen(buf), size) << "incorrect value size for " << i;
			EXPECT_EQ(0, ::memcmp(buf, buf2, size)) << "incorrect value for " << i;
		}
	}

	// db.dump();
}

TEST(ptnk, single_big_tx)
{
	DB db;
	const int NUM_KEYS = 1000;

	int ord[NUM_KEYS];
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		ord[i] = i;
	}
	for(int i = 0; i < 10000; ++ i)
	{
		int x = rand() % NUM_KEYS;	
		int y = rand() % NUM_KEYS;
		std::swap(ord[x], ord[y]);
	}

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		for(int j = 0; j < NUM_KEYS; ++ j)
		{
			int i = ord[j];

			char buf[8]; sprintf(buf, "%u", i);
			tx->put_k32u(i, cstr2ref(buf));
		}

		ASSERT_TRUE(tx->tryCommit()) << "tx failed to commit!!!";

		// tx->dumpStat();
	}
	// db.dumpStat();

	if(NUM_KEYS < 30) db.dumpAll();

	// std::cout << "do rebase!!!" << std::endl;
	db.rebase();

	if(NUM_KEYS < 30) db.dumpAll();

	Buffer v;
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char bufCorrect[8];
		sprintf(bufCorrect, "%u", i);

		db.get_k32u(i, &v);
		EXPECT_TRUE(v.isValid()) << "key " << i << " not found";
		if(v.isValid())
		{
			EXPECT_EQ((ssize_t)::strlen(bufCorrect), v.valsize());
			v.makeNullTerm();
			EXPECT_STREQ(bufCorrect, v.get());
		}
	}
}

TEST(ptnk, rebase_multiple)
{
	DB db;
	const int NUM_KEYS = 30;
	
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

			// make sure that keys [0..i-1] are readable
			for(int j = 0; j < i; ++ j)
			{
				char bufCor[8]; sprintf(bufCor, "%d", j);

				Buffer bufGet;
				ssize_t s = tx->get_k32u(j, bufGet.wref()); bufGet.setValsize(s);
				EXPECT_NE(-1, s) << "entry for key " << j << " not found";
				bufGet.makeNullTerm();
				EXPECT_EQ((ssize_t)::strlen(bufCor), s) << "entry for key " << j << " has invalid length";
				EXPECT_STREQ(bufCor, bufGet.get()) << "entr for key " << j << " is corrupted";
			}

			// put key i
			{
				char bufPut[8];
				sprintf(bufPut, "%d", i);

				tx->put_k32u(i, cstr2ref(bufPut));
			}
			ASSERT_TRUE(tx->tryCommit()) << "put " << i << " failed to commit";
		}

		if(i % 3 == 1)
		{
			db.rebase();
		}
	}

	// db.dumpAll();

	// make sure that keys [0..NUM_KEYS] are readable
	Buffer bufGet;
	for(int j = 0; j < NUM_KEYS; ++ j)
	{
		char bufCor[8]; sprintf(bufCor, "%d", j);

		ssize_t s = db.get_k32u(j, bufGet.wref()); bufGet.setValsize(s);
		EXPECT_NE(-1, s);
		EXPECT_EQ((ssize_t)::strlen(bufCor), s); 
		bufGet.makeNullTerm();
		EXPECT_STREQ(bufCor, bufGet.get());
	}
}

TEST(ptnk, intensive_rebase)
{
	DB db;
	const int NUM_KEYS = 1000;

	int ord[NUM_KEYS];
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		ord[i] = i;
	}
	for(int i = 0; i < 10000; ++ i)
	{
		int x = rand() % NUM_KEYS;	
		int y = rand() % NUM_KEYS;
		std::swap(ord[x], ord[y]);
	}

	for(int j = 0; j < NUM_KEYS; ++ j)
	{
		int i = ord[j];

		char buf[8];
		sprintf(buf, "%u", i);
		db.put_k32u(i, BufferCRef(buf, ::strlen(buf)+1));

		if(j % 10 == 1)
		{	
			Buffer v;
			for(int kk = 0; kk <= j; ++ kk)
			{
				int k = ord[kk];
				char bufCorrect[8];
				sprintf(bufCorrect, "%u", k);

				db.get_k32u(k, &v);
				EXPECT_TRUE(v.isValid()) << "value for " << k << " not found";
				if(v.isValid())
				{
					EXPECT_EQ((ssize_t)::strlen(bufCorrect)+1, v.valsize()) << "value for " << k << " invalid length";
					v.makeNullTerm();
					EXPECT_STREQ(bufCorrect, v.get()) << "value for " << k << " mksmatch";
				}
			}

			db.rebase();

			for(int kk = 0; kk <= j; ++ kk)
			{
				int k = ord[kk];
				char bufCorrect[8];
				sprintf(bufCorrect, "%u", k);

				db.get_k32u(k, &v);
				EXPECT_TRUE(v.isValid()) << "value for " << k << " not found";
				if(v.isValid())
				{
					EXPECT_EQ((ssize_t)::strlen(bufCorrect)+1, v.valsize()) << "value for " << k << " invalid length";
					v.makeNullTerm();
					EXPECT_STREQ(bufCorrect, v.get()) << "value for " << k << " mksmatch";
				}
			}
		}
	}
}

TEST(ptnk, DISABLED_million_key_put_get)
{
	DB db("/home/kouhei/work/ssd/test.ptnk", ODEFAULT | OTRUNCATE);
	const int NUM_KEYS = 1000000;

	for(int i = 0; i < NUM_KEYS;)
	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
		
		for(int j = 0; j < 100000; ++ j)
		{
			char buf[8];
			sprintf(buf, "%u", i);
			tx->put_k32u(i, cstr2ref(buf));

			++ i;
		}

		EXPECT_TRUE(tx->tryCommit());
	}
	return;

	Buffer v(32);
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char bufCorrect[8];
		sprintf(bufCorrect, "%u", i);

		db.get_k32u(i, &v);
		ASSERT_TRUE(v.isValid());
		EXPECT_EQ((ssize_t)::strlen(bufCorrect)+1, v.valsize()) << "incorrect val length for " << i;
		v.makeNullTerm();
		EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
	}
}

TEST(ptnk, save_load)
{
	t_mktmpdir("./_testtmp");

	const int NUM_KEYS = 100;

	// create dbfile
	{
		ptnk_opts_t opts = OWRITER | OCREATE | OTRUNCATE | OAUTOSYNC;
		DB db("./_testtmp/savetest.ptnk", opts);

		for(int i = 0; i < NUM_KEYS;)
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
				
			for(int j = 0; j < (NUM_KEYS/10); ++ j, ++ i)
			{
				char buf[8];
				sprintf(buf, "%u", i);
				tx->put_k32u(i, cstr2ref(buf));
			}

			EXPECT_TRUE(tx->tryCommit());
		}
		db.dumpStat();
	}
	
	// load dbfile
	{
		ptnk_opts_t opts = OWRITER | OAUTOSYNC;
		DB db("./_testtmp/savetest.ptnk", opts);

		db.dumpStat();
		// db.dumpAll();

		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

		Buffer v;
		for(int i = 0; i < NUM_KEYS; ++ i)
		{
			char bufCorrect[8];
			sprintf(bufCorrect, "%u", i);

			db.get_k32u(i, &v);
			ASSERT_TRUE(v.isValid()) << "value not found for " << i;
			EXPECT_EQ((ssize_t)::strlen(bufCorrect), v.valsize()) << "incorrect val length for " << i;
			v.makeNullTerm();
			EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
		}

		db.put_k32u(1234567, cstr2ref("testval"));	
		{
			Buffer v;
			db.get_k32u(1234567, &v);	
			ASSERT_TRUE(v.isValid());
			v.makeNullTerm();
			EXPECT_STREQ("testval", v.get()) << "incorrect val str";
		}
		db.rebase();
		{
			db.get_k32u(1234567, &v);	
			ASSERT_TRUE(v.isValid());
			v.makeNullTerm();
			EXPECT_STREQ("testval", v.get()) << "incorrect val str";
		}
	}
}

TEST(ptnk, CompMap)
{
	std::vector<local_pgid_t> marked;
	boost::scoped_ptr<PageIO> pio(new PageIOMem);

	const uint64_t numOrigPages = 100000;
	CompMap cm;

	cm.initPages(numOrigPages, pio.get());
	for(int i = 0; i < 10000; ++ i)
	{
		local_pgid_t lid = rand() % numOrigPages;
		cm.mark(lid, pio.get());
		marked.push_back(lid);
	}
	cm.fillOffsetComps(pio.get());

	std::sort(marked.begin(), marked.end());
	uint64_t c = 0;
	for(unsigned int i = 0; i < marked.size(); ++ i)
	{
		local_pgid_t trans = cm.translate(marked[i], pio.get());
		EXPECT_EQ(c, trans) << c << " != " << trans << " <- " << marked[i] << std::endl;	
	
		if(marked[i] != marked[i+1]) ++ c;
	}
}

TEST(ptnk, multitable)
{
	DB db;

	Buffer v;

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

		tx->put(cstr2ref("a"), cstr2ref("default_val"));

		tx->tableCreate(cstr2ref("test"));
		tx->put(cstr2ref("test"), cstr2ref("a"), cstr2ref("test_val"));

		tx->get(cstr2ref("default"), cstr2ref("a"), &v);
		ASSERT_TRUE(v.isValid());
		ASSERT_TRUE(bufeq(cstr2ref("default_val"), v.rref()));

		tx->get(cstr2ref("test"), cstr2ref("a"), &v);
		ASSERT_TRUE(v.isValid());
		ASSERT_TRUE(bufeq(cstr2ref("test_val"), v.rref()));

		ASSERT_TRUE(tx->tryCommit());
	}

	// check
	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

		tx->get(cstr2ref("default"), cstr2ref("a"), &v);
		ASSERT_TRUE(v.isValid());
		ASSERT_TRUE(bufeq(cstr2ref("default_val"), v.rref()));

		tx->get(cstr2ref("test"), cstr2ref("a"), &v);
		ASSERT_TRUE(v.isValid());
		ASSERT_TRUE(bufeq(cstr2ref("test_val"), v.rref()));
	}
}

TEST(ptnk, PartitionedPageIO_scanfile)
{
	t_mktmpdir("./_testtmp");
	t_makedummyfile("./_testtmp/ppioscanfile.000.ptnk");
	t_makedummyfile("./_testtmp/ppioscanfile.001.ptnk");
	t_makedummyfile("./_testtmp/ppioscanfile.abc.ptnk");
	t_makedummyfile("./_testtmp/ppioscanfile.ffe.ptnk");
	t_makedummyfile("./_testtmp/ppioscanfile.fff.ptnk");

	boost::scoped_ptr<PartitionedPageIO> ppio(new PartitionedPageIO("_testtmp/ppioscanfile", ODEFAULT));
	EXPECT_EQ(4U, ppio->numPartitions_());
}

TEST(ptnk, PartitionedPageIO_singlepart)
{
	t_mktmpdir("./_testtmp");

	const int NUM_KEYS = 100;

	// create dbfile
	{
		ptnk_opts_t opts = OWRITER | OCREATE | OTRUNCATE | OAUTOSYNC | OPARTITIONED;
		DB db("./_testtmp/ppiosingle", opts);

		for(int i = 0; i < NUM_KEYS;)
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
				
			for(int j = 0; j < (NUM_KEYS/10); ++ j, ++ i)
			{
				char buf[8];
				sprintf(buf, "%u", i);
				tx->put_k32u(i, cstr2ref(buf));
			}

			EXPECT_TRUE(tx->tryCommit());
		}
		// db.dumpStat();
	}
	
	// load dbfile
	{
		ptnk_opts_t opts = OWRITER | OAUTOSYNC | OPARTITIONED;
		DB db("./_testtmp/ppiosingle", opts);

		// db.dumpStat();
		// db.dumpAll();

		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

		Buffer v;
		for(int i = 0; i < NUM_KEYS; ++ i)
		{
			char bufCorrect[8];
			sprintf(bufCorrect, "%u", i);

			db.get_k32u(i, &v);
			ASSERT_TRUE(v.isValid()) << "value not found for " << i;
			EXPECT_EQ((ssize_t)::strlen(bufCorrect), v.valsize()) << "incorrect val length for " << i;
			v.makeNullTerm();
			EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
		}
	}
}

TEST(ptnk, PartitionedPageIO_newpart)
{
	t_mktmpdir("./_testtmp");

	const int NUM_KEYS = 100;

	// create dbfile
	{
		ptnk_opts_t opts = OWRITER | OCREATE | OTRUNCATE | OAUTOSYNC | OPARTITIONED;
		DB db("./_testtmp/ppiomulti", opts);

		for(int i = 0; i < NUM_KEYS;)
		{
			for(int j = 0; j < (NUM_KEYS/10); ++ j, ++ i)
			{
				boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
				char buf[8];
				sprintf(buf, "%u", i);
				tx->put_k32u(i, cstr2ref(buf));
				EXPECT_TRUE(tx->tryCommit());
			}
			db.newPart();
		}
		// db.dumpStat();
	}
	
	// load dbfile
	{
		ptnk_opts_t opts = OWRITER | OAUTOSYNC | OPARTITIONED;
		DB db("./_testtmp/ppiomulti", opts);

		// db.dumpStat();
		// db.dumpAll();

		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

		Buffer v;
		for(int i = 0; i < NUM_KEYS; ++ i)
		{
			char bufCorrect[8];
			sprintf(bufCorrect, "%u", i);

			db.get_k32u(i, &v);
			ASSERT_TRUE(v.isValid()) << "value not found for " << i;
			EXPECT_EQ((ssize_t)::strlen(bufCorrect), v.valsize()) << "incorrect val length for " << i;
			v.makeNullTerm();
			EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
		}
	}
}

struct alloc_pg
{
	PageIO* pio;
	page_id_t* ary;
	size_t count;

	alloc_pg(PageIO* pio_, page_id_t* ary_, size_t count_)
	:	pio(pio_), ary(ary_), count(count_)
	{ /* NOP */ }

	void operator()()
	{
		for(unsigned int i = 0; i < count; ++ i)
		{
			ary[i] = pio->newPage().second;
		}
	}
};

TEST(ptnk, PageIOMem_multithread)
{
	boost::scoped_ptr<PageIO> pio(new PageIOMem);
	
	const int NUM_THREAD = 10;
	const size_t PG_PER_THREAD = 10000;
	const size_t c = NUM_THREAD * PG_PER_THREAD;

	boost::scoped_array<page_id_t> check(new page_id_t[c]);
	boost::thread_group tg;
	for(int i = 0; i < NUM_THREAD; ++ i)
	{
		tg.create_thread(alloc_pg(pio.get(), &check[i * PG_PER_THREAD], PG_PER_THREAD));
	}
	tg.join_all();

#if 0
	FILE* f = ::fopen("allocdump.txt", "w+");
	for(unsigned int i = 0; i < c; ++ i)
	{
		::fprintf(f, "%llu\n", check[i]);
	}
	::fclose(f);
#endif

	std::sort(&check[0], &check[c]);
	for(unsigned int i = 1; i < c; ++ i)
	{
		EXPECT_NE(check[i-1], check[i]);
	}
}

TEST(ptnk, commit_fail_over_rebase)
{
	DB db;
	for(int i = 0; i < 10; ++ i)
	{
		char buf[8];
		sprintf(buf, "%u", i);
		db.put_k32u(i, cstr2ref(buf));
	}

	{
		boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

		tx->put_k32u(11, cstr2ref("test"));

		db.rebase();

		ASSERT_FALSE(tx->tryCommit());
	}
}

struct put_ary_db
{
	DB& db;
	const int* ary;
	size_t count;

	put_ary_db(DB& db_, const int* ary_, size_t count_)
	:	db(db_), ary(ary_), count(count_)
	{ /* NOP */ }

	void operator()()
	{
		for(unsigned int i = 0; i < count; ++ i)
		{
			int k = ary[i];

			char buf[8];
			sprintf(buf, "%u", k);

			for(;;)
			{
				boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

				tx->put_k32u(k, cstr2ref(buf));

				if(tx->tryCommit()) break;
			}

			// if(rand() % 4 == 0) db.rebase();
		}
	}
};

// #define DUMP_MTPUT

TEST(ptnk, multithread_put)
{
#ifdef DUMP_MTPUT
	DB db("./_testtmp/mtput.ptnk", OWRITER | OCREATE | OTRUNCATE);
#else
	DB db;
#endif
	const int NUM_KEYS = 30000;
	const int NUM_THREADS = 8;
	const int NUM_KEYS_PER_TH = NUM_KEYS / NUM_THREADS;
	SETUP_ORD(NUM_KEYS);

	boost::thread_group tg;
	for(int i = 0; i < NUM_THREADS; ++ i)
	{
		tg.create_thread(put_ary_db(db, &ord[NUM_KEYS_PER_TH * i], NUM_KEYS_PER_TH));
	}
	tg.join_all();

	if(0){
		FILE* fp = fopen("graphdump/mt.gv", "w");
		db.dumpGraph(fp);
		fclose(fp);
	}

	FILE* fplist = NULL;
#ifdef DUMP_MTPUT
	fplist = fopen("_testtmp/mtkeys", "w");
#endif

	Buffer v(32);
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char bufCorrect[8];
		sprintf(bufCorrect, "%u", i);

		db.get_k32u(i, &v);
		EXPECT_TRUE(v.isValid()) << "value not found for " << i;
		if(v.isValid())
		{
			EXPECT_EQ((ssize_t)::strlen(bufCorrect), v.valsize()) << "incorrect val length for " << i;
			v.makeNullTerm();
			EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
		}

		if(fplist) fprintf(fplist, "%u %u\n", i, v.isValid());
	}
	if(fplist) fclose(fplist);
}

TEST(ptnk, ndbm_api)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/ndbmtest.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	const int NUM_KEYS = 100;

	// check ptnk_store
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char buf[8];
		sprintf(buf, "%u", i);

		ptnk_datum_t key = {(char*)&i, sizeof(int)};
		ptnk_datum_t value = {buf, ::strlen(buf)+1};
		EXPECT_TRUE(::ptnk_put(db, key, value, PUT_INSERT));
	}

	// check ptnk_fetch
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		char bufCorrect[8];
		sprintf(bufCorrect, "%u", i);

		ptnk_datum_t key = {(char*)&i, sizeof(int)};
		ptnk_datum_t val = ::ptnk_get(db, key);

		EXPECT_STREQ(val.dptr, bufCorrect) << "invalid value for " << i;
	}

	::ptnk_close(db);
}

TEST(ptnk, capi_tx)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_tx.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* txA = ::ptnk_tx_begin(db);
	ptnk_tx_t* txB = ::ptnk_tx_begin(db);

	// put random record in txA
	EXPECT_TRUE(::ptnk_tx_put_cstr(txA, "key", "value", PUT_INSERT));

	{
		ptnk_datum_t k = {(char*)"key", 3};
		ptnk_datum_t v = ::ptnk_tx_get(txA, k);
		EXPECT_EQ(5, v.dsize);

		EXPECT_STREQ("value", ::ptnk_tx_get_cstr(txA, "key"));
	}

	// get the record from txB -> should fail
	{
		ptnk_datum_t k = {(char*)"key", 3};
		ptnk_datum_t v = ::ptnk_tx_get(txB, k);
		EXPECT_EQ(-1, v.dsize);

		EXPECT_STREQ(NULL, ::ptnk_tx_get_cstr(txB, "key"));
	}

	EXPECT_NE(0, ::ptnk_tx_end(txA, PTNK_TX_COMMIT)) << "txA failed";
	EXPECT_EQ(0, ::ptnk_tx_end(txB, PTNK_TX_ABORT));

	// the record put is applied to db after successful commit of txA
	EXPECT_STREQ("value", ::ptnk_get_cstr(db, "key"));

	::ptnk_close(db);
}

TEST(ptnk, capi_table)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_tx.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table_A"));
	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table_B"));

	ptnk_table_t* tableA = ::ptnk_table_open_cstr("table_A");
	ptnk_table_t* tableB = ::ptnk_table_open_cstr("table_B");

	::ptnk_tx_table_put_cstr(tx, tableA, "key", "valueA", PUT_INSERT);
	::ptnk_tx_table_put_cstr(tx, tableB, "key", "valueB", PUT_INSERT);

	EXPECT_STREQ("valueA", ::ptnk_tx_table_get_cstr(tx, tableA, "key"));
	EXPECT_STREQ("valueB", ::ptnk_tx_table_get_cstr(tx, tableB, "key"));

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";

	::ptnk_table_close(tableA);
	::ptnk_table_close(tableB);

	::ptnk_close(db);
}

TEST(ptnk, capi_cur)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_cur.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table"));
	ptnk_table_t* t = ::ptnk_table_open_cstr("table");

	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "a", "A", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "b", "B", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "c", "C", PUT_INSERT));

	EXPECT_STREQ("A", ::ptnk_tx_table_get_cstr(tx, t, "a"));
	EXPECT_STREQ("B", ::ptnk_tx_table_get_cstr(tx, t, "b"));
	EXPECT_STREQ("C", ::ptnk_tx_table_get_cstr(tx, t, "c"));

	{
		ptnk_cur_t* c = ::ptnk_cur_front(tx, t);
		EXPECT_TRUE(c);

		ptnk_datum_t k, v;

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('a', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('A', *v.dptr); EXPECT_EQ(1, v.dsize);

		v.dptr = (char*)"X";
		v.dsize = 1;
		EXPECT_NE(0, ::ptnk_cur_put(c, v));
		
		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('a', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('X', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('b', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('B', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('c', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('C', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_EQ(0, ::ptnk_cur_next(c));

		::ptnk_cur_close(c);
	}
	{
		ptnk_cur_t* c = ::ptnk_cur_front(tx, t);
		EXPECT_TRUE(c);

		ptnk_datum_t k, v;
		
		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('a', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('X', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('b', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('B', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('c', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('C', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_EQ(0, ::ptnk_cur_next(c));

		::ptnk_cur_close(c);
	}

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";

	::ptnk_table_close(t);

	::ptnk_close(db);
}

TEST(ptnk, capi_cur_samekey)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_cur.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* tx = ::ptnk_tx_begin(db);
	EXPECT_TRUE(tx);

	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table"));
	ptnk_table_t* t = ::ptnk_table_open_cstr("table");

	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "", "A", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "", "B", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "", "C", PUT_INSERT));

	{
		ptnk_cur_t* c = ::ptnk_cur_front(tx, t);
		EXPECT_TRUE(c);

		ptnk_datum_t k, v;

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('A', *v.dptr); EXPECT_EQ(1, v.dsize);

		v.dptr = (char*)"X";
		v.dsize = 1;
		EXPECT_NE(0, ::ptnk_cur_put(c, v));
		
		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('X', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('B', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('C', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_EQ(0, ::ptnk_cur_next(c));

		::ptnk_cur_close(c);
	}
	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";

	tx = ::ptnk_tx_begin(db);
	EXPECT_TRUE(tx);

	{
		ptnk_cur_t* c = ::ptnk_cur_front(tx, t);
		EXPECT_TRUE(c);

		ptnk_datum_t k, v;
		
		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('X', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('B', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ(0, k.dsize);
		EXPECT_EQ('C', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_EQ(0, ::ptnk_cur_next(c));

		::ptnk_cur_close(c);
	}

	EXPECT_EQ(0, ::ptnk_tx_end(tx, PTNK_TX_ABORT)) << "tx failed";

	::ptnk_table_close(t);

	::ptnk_close(db);
}

TEST(ptnk, capi_cur_delete)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_cur_delete.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table"));
	ptnk_table_t* t = ::ptnk_table_open_cstr("table");

	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "a", "A", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "b", "B", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "c", "C", PUT_INSERT));

	EXPECT_STREQ("A", ::ptnk_tx_table_get_cstr(tx, t, "a"));
	EXPECT_STREQ("B", ::ptnk_tx_table_get_cstr(tx, t, "b"));
	EXPECT_STREQ("C", ::ptnk_tx_table_get_cstr(tx, t, "c"));

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";
	tx = ::ptnk_tx_begin(db);

	{
		ptnk_cur_t* c = ::ptnk_cur_front(tx, t);
		EXPECT_TRUE(c);

		ptnk_datum_t k, v;

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('a', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('A', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_delete(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('b', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('B', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_NE(0, ::ptnk_cur_next(c));

		EXPECT_NE(0, ::ptnk_cur_get(&k, &v, c));
		EXPECT_EQ('c', *k.dptr); EXPECT_EQ(1, k.dsize);
		EXPECT_EQ('C', *v.dptr); EXPECT_EQ(1, v.dsize);

		EXPECT_EQ(0, ::ptnk_cur_next(c));

		::ptnk_cur_close(c);
	}

	EXPECT_EQ(NULL, ::ptnk_tx_table_get_cstr(tx, t, "a"));

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";

	::ptnk_table_close(t);

	::ptnk_close(db);
}

TEST(ptnk, capi_cur_query)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_cur_query.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table"));
	ptnk_table_t* t = ::ptnk_table_open_cstr("table");

	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "a", "A", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "b", "B", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "c", "C", PUT_INSERT));

	EXPECT_STREQ("A", ::ptnk_tx_table_get_cstr(tx, t, "a"));
	EXPECT_STREQ("B", ::ptnk_tx_table_get_cstr(tx, t, "b"));
	EXPECT_STREQ("C", ::ptnk_tx_table_get_cstr(tx, t, "c"));

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";
	tx = ::ptnk_tx_begin(db);

	{
		ptnk_datum_t k = {(char*)"b", 1};
		ptnk_cur_t* c = ::ptnk_query(tx, t, k, MATCH_EXACT);

		const char *key, *value;
		EXPECT_NE(0, ::ptnk_cur_get_cstr(&key, &value, c));

		EXPECT_STREQ("b", key);
		EXPECT_STREQ("B", value);

		::ptnk_cur_close(c);
	}

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";

	::ptnk_table_close(t);

	::ptnk_close(db);
}

TEST(ptnk, ptnk_capi_delete_all_records)
{
	t_mktmpdir("./_testtmp");

	ptnk_db_t* db = ::ptnk_open("./_testtmp/capi_delall.ptnk", ODEFAULT, 0644);
	ASSERT_TRUE(db);

	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	EXPECT_NE(0, ::ptnk_tx_table_create_cstr(tx, "table"));
	ptnk_table_t* t = ::ptnk_table_open_cstr("table");

	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "a", "A", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "b", "B", PUT_INSERT));
	EXPECT_NE(0, ::ptnk_tx_table_put_cstr(tx, t, "c", "C", PUT_INSERT));

	EXPECT_STREQ("A", ::ptnk_tx_table_get_cstr(tx, t, "a"));
	EXPECT_STREQ("B", ::ptnk_tx_table_get_cstr(tx, t, "b"));
	EXPECT_STREQ("C", ::ptnk_tx_table_get_cstr(tx, t, "c"));

	{
		ptnk_cur_t* c = ::ptnk_cur_front(tx, t);
		EXPECT_TRUE(c);

		EXPECT_NE(0, ::ptnk_cur_delete(c));
		EXPECT_NE(0, ::ptnk_cur_delete(c));
		EXPECT_EQ(0, ::ptnk_cur_delete(c));

		::ptnk_cur_close(c);
	}

	EXPECT_NE(0, ::ptnk_tx_end(tx, PTNK_TX_COMMIT)) << "tx failed";

	::ptnk_close(db);
}
