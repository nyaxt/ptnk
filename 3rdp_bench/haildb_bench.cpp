// this bench prog based on ib_test1.c from haildb
#include "../bench_tmpl.h"

/***********************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

************************************************************************/

#define NDEBUG
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <libgen.h>

#include <haildb.h>

#define DATABASE	"test"
#define TABLE		"t"

/* A row from our test table. */
typedef struct row_t {
	char		k[9];
	ib_u32_t	v;
} row_t;

/*********************************************************************
Create an InnoDB database (sub-directory). */
static
ib_err_t
create_database(
/*============*/
	const char*	name)
{
	ib_bool_t	err;

	err = ib_database_create(name);
	assert(err == IB_TRUE);

	return(DB_SUCCESS);
}

static
ib_err_t
create_table(
	const char*	dbname,			/*!< in: database name */
	const char*	name)			/*!< in: table name */
{
	ib_trx_t	ib_trx;
	ib_id_t		table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	/* Pass a table page size of 0, ie., use default page size. */
	err = ib_table_schema_create(
		table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "k",
		IB_VARCHAR, IB_COL_NONE, 0, 8);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "v",
		IB_INT, IB_COL_UNSIGNED, 0, 4);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY_KEY", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col(ib_idx_sch, "k", 0);
	assert(err == DB_SUCCESS);

	err = ib_index_schema_set_clustered(ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* create table */
	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	err = ib_schema_lock_exclusive(ib_trx);
	assert(err == DB_SUCCESS);

	err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
	assert(err == DB_SUCCESS);

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	if (ib_tbl_sch != NULL) {
		ib_table_schema_delete(ib_tbl_sch);
	}

	return(err);
}

/*********************************************************************
Open a table and return a cursor for the table. */
static
ib_err_t
open_table(
/*=======*/
	const char*	dbname,		/*!< in: database name */
	const char*	name,		/*!< in: table name */
	ib_trx_t	ib_trx,		/*!< in: transaction */
	ib_crsr_t*	crsr)		/*!< out: innodb cursor */
{
	ib_err_t	err = DB_SUCCESS;
	char		table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif
	err = ib_cursor_open_table(table_name, ib_trx, crsr);
	assert(err == DB_SUCCESS);

	return(err);
}

static const char log_group_home_dir[] = "log";
static const char data_file_path[] = "ibdata1:32M:autoextend";
static void test_configure(void)
{
	ib_err_t	err;

	mkdir(log_group_home_dir, 0755);

#ifndef __WIN__
	err = ib_cfg_set_text("flush_method", "O_DIRECT");
	assert(err == DB_SUCCESS);
#else
	err = ib_cfg_set_text("flush_method", "async_unbuffered");
	assert(err == DB_SUCCESS);
#endif

	err = ib_cfg_set_int("log_files_in_group", 2);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("log_file_size", 32 * 1024 * 1024);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("log_buffer_size", 24 * 16384);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("buffer_pool_size", 1024 * 1024 * 1024);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("additional_mem_pool_size", 1024 * 1024 * 1024);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("flush_log_at_trx_commit", do_sync ? 1 : 0);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("file_io_threads", 4);
	assert(err == DB_READONLY);

	err = ib_cfg_set_int("lock_wait_timeout", 60);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_int("open_files", 300);
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_bool_on("doublewrite");
	assert(err == DB_SUCCESS);

	// err = ib_cfg_set_bool_on("checksums");
	// assert(err == DB_SUCCESS);

	err = ib_cfg_set_bool_on("rollback_on_timeout");
	assert(err == DB_SUCCESS);

	// err = ib_cfg_set_bool_on("print_verbose_log");
	// assert(err == DB_SUCCESS);

	err = ib_cfg_set_bool_on("file_per_table");
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_text("data_home_dir", "./");
	assert(err == DB_SUCCESS);

	err = ib_cfg_set_text("log_group_home_dir", log_group_home_dir);

	if (err != DB_SUCCESS) {
		fprintf(stderr,
			"syntax error in log_group_home_dir, or a "
			  "wrong number of mirrored log groups\n");
		exit(1);
	}

	err = ib_cfg_set_text("data_file_path", data_file_path);

	if (err != DB_SUCCESS) {
		fprintf(stderr,
			"InnoDB: syntax error in data_file_path\n");
		exit(1);
	}
}

void
run_bench(void)
{
	ib_err_t	err;
	ib_crsr_t	crsr;
	ib_trx_t	ib_trx;

	char strdir[4096]; ::strcpy(strdir, dbfile);
	if(::chdir(dirname(strdir)) != 0)
	{
		std::cout << "chdir failed" << std::endl;	
		return;
	}

	Bench b("haildb_bench", comment);
	{
		err = ib_init();
		assert(err == DB_SUCCESS);

		test_configure();

		err = ib_startup("barracuda");
		assert(err == DB_SUCCESS);

		err = create_database(DATABASE);
		assert(err == DB_SUCCESS);

		err = create_table(DATABASE, TABLE);
		assert(err == DB_SUCCESS);

		b.cp("db init");

		int ik = 0;
		if(NUM_PREW > 0)
		{
			ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
			assert(ib_trx != NULL);

			err = open_table(DATABASE, TABLE, ib_trx, &crsr);
			assert(err == DB_SUCCESS);

			// printf("Lock table in IX\n");
			err = ib_cursor_lock(crsr, IB_LOCK_IX);
			assert(err == DB_SUCCESS);

			ib_tpl_t	tpl = ib_clust_read_tuple_create(crsr);;
			assert(tpl != NULL);
			
			for(int i = 0; i < NUM_PREW; ++ i)
			{
				int k = keys[ik++];

				char buf[9];
				sprintf(buf, "%08u", k);

				err = ib_col_set_value(tpl, 0, buf, 8);
				assert(err == DB_SUCCESS);

				err = ib_col_set_value(tpl, 1, &k, 4);
				assert(err == DB_SUCCESS);
				
				err = ib_cursor_insert_row(crsr, tpl);
				assert(err == DB_SUCCESS);

				tpl = ib_tuple_clear(tpl);
				assert(tpl != NULL);
			}

			ib_tuple_delete(tpl);

			// printf("Close cursor\n");
			err = ib_cursor_close(crsr);
			assert(err == DB_SUCCESS);
			crsr = NULL;

			// printf("Commit transaction\n");
			err = ib_trx_commit(ib_trx);
			assert(err == DB_SUCCESS);

			// don't include preloading time
			fprintf(stderr, "prewrite %d keys done\n", ik);
			b.start(); b.cp("db init");
		}

		for(int itx = 0; itx < NUM_TX; ++ itx)
		{
			// printf("Begin transaction\n");
			ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
			assert(ib_trx != NULL);

			ib_tpl_t tpl;
			if(NUM_W_PER_TX > 0)
			{
				// printf("Open cursor\n");
				err = open_table(DATABASE, TABLE, ib_trx, &crsr);
				assert(err == DB_SUCCESS);

				// printf("Lock table in IX\n");
				err = ib_cursor_lock(crsr, IB_LOCK_IX);
				assert(err == DB_SUCCESS);

				tpl = ib_clust_read_tuple_create(crsr);;
				assert(tpl != NULL);
				
				for(int iw = 0; iw < NUM_W_PER_TX; ++ iw)
				{
					int k = keys[ik++];

					char buf[9];
					sprintf(buf, "%08u", k);

					err = ib_col_set_value(tpl, 0, buf, 8);
					assert(err == DB_SUCCESS);

					err = ib_col_set_value(tpl, 1, &k, 4);
					assert(err == DB_SUCCESS);
					
					err = ib_cursor_insert_row(crsr, tpl);
					assert(err == DB_SUCCESS);

					tpl = ib_tuple_clear(tpl);
					assert(tpl != NULL);
				}

				ib_tuple_delete(tpl);

				// printf("Close cursor\n");
				err = ib_cursor_close(crsr);
				assert(err == DB_SUCCESS);
				crsr = NULL;
			}

			if(NUM_R_PER_TX > 0)
			{
				// printf("Open cursor\n");
				err = open_table(DATABASE, TABLE, ib_trx, &crsr);
				assert(err == DB_SUCCESS);

				tpl = ib_clust_read_tuple_create(crsr);;
				assert(tpl != NULL);

				ib_tpl_t key_tpl = ib_sec_search_tuple_create(crsr);
				assert(key_tpl != NULL);

				for(int ir = 0; ir < NUM_R_PER_TX; ++ ir)
				{
					int k = keys[rand() % ik];

					char buf[9];
					sprintf(buf, "%08u", k);
					
					err = ib_col_set_value(key_tpl, 0, buf, 8);
					assert(err == DB_SUCCESS);

					int res = ~0;
					err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
					assert(res == 0);
				
					err = ib_cursor_read_row(crsr, tpl);
					assert(err == DB_SUCCESS);

					tpl = ib_tuple_clear(tpl);
					assert(tpl != NULL);
				}

				ib_tuple_delete(key_tpl);
				ib_tuple_delete(tpl);

				// printf("Close cursor\n");
				err = ib_cursor_close(crsr);
				assert(err == DB_SUCCESS);
				crsr = NULL;
			}

			// printf("Commit transaction\n");
			err = ib_trx_commit(ib_trx);
			assert(err == DB_SUCCESS);
		}
		b.cp("tx done");

		if(NUM_W_PER_TX == 0)
		{
			// avoid measuring clean up time
			b.end();b.dump();
			exit(0);
		}

		err = ib_shutdown(IB_SHUTDOWN_NORMAL);
		assert(err == DB_SUCCESS);
	}
	b.end();
	b.dump();
}
