#include "ptnk/helperthr.h"

#include <iostream>
#include <gtest/gtest.h>

using namespace ptnk;

TEST(helperthr, basic)
{
	Helper helper;

	bool b = false;
	EXPECT_FALSE(b);
	helper.enq([&b]() mutable {
		b = true;	
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	EXPECT_TRUE(b);
}

TEST(helperthr, multiple)
{
	Helper helper;

	int count = 0;
	for(int i = 0; i < 32; ++ i)
	{
		helper.enq([&count]() mutable {
			count ++;	
		});
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	EXPECT_EQ(32, count);
}
