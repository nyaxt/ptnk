#include "bench_tmpl.h"
#include "ptnk/sysutils.h"
#include "ptnk.h"

#include <boost/thread.hpp>

using namespace ptnk;

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

			// puts(buf);
			for(;;)
			{
				boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

				tx->put(BufferCRef(&k, 4), cstr2ref(buf));

				if(tx->tryCommit()) break;
			}
		}
	}
};

void
run_bench()
{
	Bench b((boost::format("%1% %2%") % PROGNAME % comment).str());
	{
		const int NUM_KEYS_PER_TH = NUM_KEYS / NUM_THREADS;
		SETUP_ORD(NUM_KEYS);

		ptnk_opts_t opts = OWRITER | OCREATE | OTRUNCATE;
		if(do_sync) opts |= OAUTOSYNC;
		
		DB db(dbfile, opts);
		// DB db;
		b.cp("db init");

		boost::thread_group tg;
		for(int i = 0; i < NUM_THREADS; ++ i)
		{
			std::cout << "t";
			tg.create_thread(put_ary_db(db, &ord[NUM_KEYS_PER_TH * i], NUM_KEYS_PER_TH));
		}
		tg.join_all();

		b.cp("tx done");
	}
	b.end();
	b.dump();

	MutexProf::dumpStatAll();
}
