#include "ptnk.h"

#include <stdio.h>

using namespace ptnk;

int
main(int argc, char* argv[])
{
	if(argc < 2)
	{
		printf("Usage: %s [ptnkdb]\n", argv[0]);	
		return 1;
	}

	try
	{
		ptnk::DB db(argv[1], OWRITER | OPARTITIONED);
		db.compact();
	}
	catch(std::exception& e)
	{
		std::cerr << e.what() << std::endl;	
	}

	return 0;
}
