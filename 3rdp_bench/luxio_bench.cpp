#include "../bench_tmpl.h"

#include <luxio/btree.h>

// adopted from luxio/example/put_test.cpp

void
run_bench()
{
	if(do_sync)
	{
		std::cerr << "no support for do_sync" << std::endl;
		exit(1);
	}

	Bench b("luxio_bench", comment);
	{
		std::auto_ptr<Lux::IO::Btree> bt(new Lux::IO::Btree(Lux::IO::CLUSTER));
		bt->open(dbfile, Lux::IO::DB_CREAT);

		b.cp("db init");
		int ik = 0;
		if(NUM_PREW > 0)
		{
			for(int i = 0; i < NUM_PREW; ++ i)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);
				Lux::IO::data_t key = {buf, strlen(buf)};
				Lux::IO::data_t val = {&k, sizeof(int)};
				bt->put(&key, &val); // insert operation
			}

			// don't include preloading time
			fprintf(stderr, "prewrite %d keys done\n", ik);
			b.start(); b.cp("db init");
		}

		for(int itx = 0; itx < NUM_TX; ++ itx)
		{
			for(int iw = 0; iw < NUM_W_PER_TX; ++ iw)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);
				Lux::IO::data_t key = {buf, strlen(buf)};
				Lux::IO::data_t val = {&k, sizeof(int)};
				bt->put(&key, &val); // insert operation
			}

			for(int ir = 0; ir < NUM_R_PER_TX; ++ ir)
			{
				int k = keys[rand() % ik];

				char buf[9]; sprintf(buf, "%08u", k);
				Lux::IO::data_t key = {buf, strlen(buf)};
				Lux::IO::data_t* val = bt->get(&key);
				
				bt->clean_data(val);
			}
		}
		b.cp("tx done");
		if(NUM_W_PER_TX == 0)
		{
			// avoid measuring clean up time
			b.end();b.dump();
			exit(0);
		}

		bt->close();
	}
	b.end();
	b.dump();
}
