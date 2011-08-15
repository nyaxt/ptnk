#include <iostream>
#include <memory>

#include "ptnk.h"

using namespace ptnk;

int
main(int argc, char* argv[])
{
	if(argc < 2)
	{
		fprintf(stderr, "usage: %s ptnkdb\n", argv[0]);
		return 1;
	}

	try
	{
		const char* dbfile = argv[1];
		DB db(dbfile);

		Buffer tablename;
		std::unique_ptr<DB::Tx> tx(db.newTransaction());
		for(int i = 0; /* NOP */; ++ i)
		{
			tx->tableGetName(i, &tablename);
			if(! tablename.isValid()) break;

			std::cout << "** Table: " << tablename.rref().inspect() << std::endl;

			Buffer k(4096), v(4096);
			std::unique_ptr<DB::Tx::cursor_t, decltype(&DB::Tx::curClose)> cur(tx->curFront(tablename.rref()), DB::Tx::curClose);
			if(! cur.get()) continue;

			do
			{
				tx->curGet(&k, &v, cur.get());
				std::cout << k.rref().inspect() << " -> " << v.rref().inspect() << std::endl;
			}
			while(tx->curNext(cur.get()));
		}
	}
	catch(std::exception& e)
	{
		std::cerr << "caught exception: " << e.what() << std::endl;	
	}
}
