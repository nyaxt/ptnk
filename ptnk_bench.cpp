#include "bench_tmpl.h"
#include "ptnk.h"

using namespace ptnk;

void
run_bench()
{
	// sleep(3);

	Bench b("ptnk_bench");
	{
		ptnk_opts_t opts = OWRITER | OCREATE | OTRUNCATE | OPARTITIONED;
		if(do_sync) opts |= OAUTOSYNC;
		
		b.start();

		DB db(dbfile, opts);
		b.cp("db init");

		int ik = 0;
		if(NUM_PREW > 0)
		{
			while(ik < NUM_PREW)
			{
				boost::scoped_ptr<DB::Tx> tx(db.newTransaction());

				for(int j = 0; j < 100; ++ j)
				{
					int k = keys[ik++];

					char buf[9]; sprintf(buf, "%08u", k);
					tx->put(BufferCRef(buf, ::strlen(buf)), BufferCRef(&k, sizeof(int)));
				}

				tx->tryCommit();
			}

			// don't include preloading time
			fprintf(stderr, "prewrite %d keys done\n", ik);
			b.start(); b.cp("db init");
		}

		Buffer v;

		for(int itx = 0; itx < NUM_TX; ++ itx)
		{
			boost::scoped_ptr<DB::Tx> tx(db.newTransaction());
			
			for(int iw = 0; iw < NUM_W_PER_TX; ++ iw)
			{
				int k = keys[ik++];

				char buf[9]; sprintf(buf, "%08u", k);
				tx->put(BufferCRef(buf, ::strlen(buf)), BufferCRef(&k, sizeof(int)));
			}

			for(int ir = 0; ir < NUM_R_PER_TX; ++ ir)
			{
				int k = keys[rand() % ik];

				char buf[9]; sprintf(buf, "%08u", k);
				tx->get(BufferCRef(&buf, ::strlen(buf)), &v);
			}

			tx->tryCommit();
			if(do_intensiverebase) db.rebase();
			// tx->dumpStat();
		}
		b.cp("tx done");
		if(NUM_W_PER_TX == 0)
		{
			// avoid measuring clean up time
			b.end();b.dump();
			exit(0);
		}

		db.rebase();
	}
	b.end();
	b.dump();

#if 0
	Buffer v(32);
	char bufCorrect[9]; bufCorrect[9] = '\0';
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		sprintf(bufCorrect, "%08u", i);

		db.get(i, v);
		ASSERT_TRUE(v.isValid());
		EXPECT_EQ(::strlen(bufCorrect)+1, v.valsize()) << "incorrect val length for " << i;
		EXPECT_STREQ(bufCorrect, v.get()) << "incorrect val str for " << i;
	}
	return 0;
#endif
}
