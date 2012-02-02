#include "../bench_tmpl.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

using namespace leveldb;

void
run_bench()
{
	Bench b("leveldb_bench", comment);
	{
		DB* db;
		Options opts;
		opts.create_if_missing = true;
		WriteOptions wopt;
		if(do_sync) wopt.sync = true;

		Status status = DB::Open(opts, dbfile, &db);
		if(! status.ok())
		{
			fprintf(stderr, "error opening db file\n");
			std::cerr << status.ToString() << std::endl;
		}
		b.cp("db init");

		int ik = 0;
		if(NUM_PREW > 0)
		{
			for(int i = 0; i < NUM_PREW; ++ i)
			{
				int k = keys[ik++];

				char buf[9]; sprintf(buf, "%08u", k);
				db->Put(wopt, Slice(buf, 8), Slice((const char*)&k, sizeof(k)));
			}

			// don't include preloading time
			fprintf(stderr, "prewrite %d keys done\n", ik);
			b.start(); b.cp("db init");
		}

		for(int itx = 0; itx < NUM_TX; ++ itx)
		{
			WriteBatch b;
			
			for(int iw = 0; iw < NUM_W_PER_TX; ++ iw)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);

				b.Put(Slice(buf, 8), Slice((char*)&k, sizeof(k)));
			}

			for(int ir = 0; ir < NUM_R_PER_TX; ++ ir)
			{
				leveldb::ReadOptions options;
				options.snapshot = db->GetSnapshot();

				int k = keys[rand() % ik];
				char buf[9];
				sprintf(buf, "%08u", k);

				std::string value;
				db->Get(options, Slice(buf, 8), &value);

				db->ReleaseSnapshot(options.snapshot);
			}

			db->Write(wopt, &b);
		}
		b.cp("tx done");

		if(NUM_W_PER_TX == 0)
		{
			// avoid measuring clean up time
			b.end();b.dump();
			exit(0);
		}
		sync();

		delete db;
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
