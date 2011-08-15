#include "../bench_tmpl.h"
#include <db_cxx.h>
#include <libgen.h>

void
run_bench()
{
	if(do_sync)
	{
		std::cerr << "sync option ignored in bdb_bench"	<< std::endl;
		exit(1);
	}
	
	uint32_t dbopt = DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE | DB_LOG_DSYNC;

	Bench b((boost::format("%1% %2%") % PROGNAME % comment).str());
	{
		DbEnv* envp = NULL;
		Db* dbp = NULL;  

		envp = new DbEnv(0);
		char strdir[4096]; ::strcpy(strdir, dbfile);
		envp->open(dirname(strdir), dbopt, 00644);

		dbp = new Db(envp, 0);
		char strbase[4096]; ::strcpy(strbase, dbfile);
		dbp->open(NULL, basename(strbase), NULL, DB_BTREE, dbopt, 00644);
		
		b.cp("db init");
		int ik = 0;

		if(NUM_PREW > 0)
		{
			DbTxn *txn = NULL;
			envp->txn_begin(NULL, &txn, 0);
			for(int i = 0; i < NUM_PREW; ++ i)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);

				Dbt key, value;
				key.set_data(buf);
				key.set_size(::strlen(buf));
				value.set_data(&k);
				value.set_size(sizeof(int));

				dbp->put(txn, &key, &value, 0);
			}
			txn->commit(0);

			// don't include preloading time
			fprintf(stderr, "prewrite %d keys done\n", ik);
			b.start(); b.cp("db init");
		}

		for(int itx = 0; itx < NUM_TX; ++ itx)
		{
			DbTxn *txn = NULL;
			envp->txn_begin(NULL, &txn, 0);
			
			for(int iw = 0; iw < NUM_W_PER_TX; ++ iw)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);

				Dbt key, value;
				key.set_data(buf);
				key.set_size(::strlen(buf));
				value.set_data(&k);
				value.set_size(sizeof(int));

				dbp->put(txn, &key, &value, 0);
			}

			for(int ir = 0; ir < NUM_R_PER_TX; ++ ir)
			{
				int k = keys[rand() % ik];

				char buf[9];
				sprintf(buf, "%08u", k);

				Dbt key, value;
				value.set_data(buf);
				value.set_size(::strlen(buf));
				dbp->get(txn, &key, &value, 0);
			}

			try
			{
				txn->commit(0);
				txn = NULL;
			}
			catch(...)
			{
				std::cerr << "txn commit failed!" << std::endl;	
				exit(1);
			}
		}
		b.cp("tx done");

		if(NUM_W_PER_TX == 0)
		{
			// avoid measuring clean up time
			b.end();b.dump();
			exit(0);
		}

		dbp->close(0);
		envp->close(0);
	}
	b.end();
	b.dump();
}
