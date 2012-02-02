#include "ptnk/helperthr.h"

#include <iostream>
#include <unistd.h>
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

	// std::this_thread::sleep_for(std::chrono::milliseconds(100)); // sleep_for not enabled on some gcc
	usleep(100 * 1000);
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

	// std::this_thread::sleep_for(std::chrono::milliseconds(100));
	usleep(100 * 1000);
	EXPECT_EQ(32, count);
}
