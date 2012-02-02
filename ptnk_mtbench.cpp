#include "bench_tmpl.h"
#include "ptnk/sysutils.h"
#include "ptnk.h"

#include <thread>

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

unsigned int g_confl = 0;

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
				unique_ptr<DB::Tx> tx(db.newTransaction());

				tx->put(BufferCRef(&k, 4), cstr2ref(buf));

				if(tx->tryCommit()) break;
				g_confl ++;
			}
		}
	}
};

void
run_bench()
{
	stageprof_init();

	Bench b((boost::format("%1% %2%") % PROGNAME % comment).str());
	{
		const int NUM_KEYS_PER_TH = NUM_KEYS / NUM_THREADS;

		ptnk_opts_t opts = OWRITER | OCREATE | OTRUNCATE | OPARTITIONED | OHELPERTHREAD;
		if(do_sync) opts |= OAUTOSYNC;
		
		DB db(dbfile, opts);
		// DB db;
		// b.cp("db init");
		b.start();

		typedef unique_ptr<std::thread> Pthread;
		std::vector<Pthread> tg;
		for(int i = 0; i < NUM_THREADS; ++ i)
		{
			tg.push_back(Pthread(new std::thread(put_ary_db(db, &keys[NUM_KEYS_PER_TH * i], NUM_KEYS_PER_TH))));
		}
		for(auto& t: tg) t->join();

		b.cp("tx done");
	}
	b.end();
	b.dump();

	MutexProf::dumpStatAll();
	std::cout << "# confl: " << g_confl << std::endl;
	std::cout << "# keys: " << NUM_KEYS << std::endl;

	stageprof_dump();
}
