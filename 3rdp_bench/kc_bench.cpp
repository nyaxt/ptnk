#include "../bench_tmpl.h"
#include <kchashdb.h>

using namespace kyotocabinet;

void
run_bench()
{
	uint32_t dbopt = TreeDB::OWRITER | TreeDB::OCREATE | TreeDB::OTRUNCATE;
	if(do_sync) dbopt |= TreeDB::OAUTOSYNC;

	Bench b((boost::format("kc_bench %1%") % comment).str());
	{
		TreeDB db;
		db.open(dbfile, dbopt);
		b.cp("db init");

		int ik = 0;
		if(NUM_PREW > 0)
		{
			for(int i = 0; i < NUM_PREW; ++ i)
			{
				int k = keys[ik++];

				char buf[9]; sprintf(buf, "%08u", k);
				db.set(buf, 8, (char*)&k, sizeof(k));
			}

			// don't include preloading time
			fprintf(stderr, "prewrite %d keys done\n", ik);
			b.start(); b.cp("db init");
		}

		for(int itx = 0; itx < NUM_TX; ++ itx)
		{
			db.begin_transaction();
			
			for(int iw = 0; iw < NUM_W_PER_TX; ++ iw)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);

				db.set(buf, 8, (char*)&k, sizeof(k));
			}

			for(int ir = 0; ir < NUM_R_PER_TX; ++ ir)
			{
				int k = keys[rand() % ik];
				char buf[9];
				sprintf(buf, "%08u", k);

				char v[9];

				db.get(buf, 8, v, 9);
			}

			db.end_transaction();
		}
		b.cp("tx done");

		if(NUM_W_PER_TX == 0)
		{
			// avoid measuring clean up time
			b.end();b.dump();
			exit(0);
		}
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
#endif
}
