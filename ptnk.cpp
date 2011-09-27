#include "ptnk.h"

#include <errno.h>
#include <fcntl.h>

#define PTNK_READ_BUF_SIZE 4096
// #define PTNK_LOG_ALL_CAPI

#ifdef PTNK_LOG_ALL_CAPI
#include <stdio.h>
#define LOG_OUTF(...) do { fprintf(stdout, "ptnk CAPI: " __VA_ARGS__); fflush(stdout); } while(0)
#else
#define LOG_OUTF(...) do {} while(0)
#endif

struct ptnk_db
{
	//! pointer to C++ impl.
	ptnk::DB* impl;

	//! error code
	int ptnk_errno;

	//! buffer used in read operations (mostly value)
	ptnk::Buffer read_buf;

	//! buffer used in read operations (mostly key)
	ptnk::Buffer read_buf2;

	ptnk_db(ptnk::DB* impl_)
	:	impl(impl_),
		ptnk_errno(0),
		read_buf(PTNK_READ_BUF_SIZE),
		read_buf2(PTNK_READ_BUF_SIZE)
	{ /* NOP */	}

	~ptnk_db()
	{
		delete impl;	
	}
};

struct ptnk_tx
{
	//! pointer to C++ impl.
	ptnk::DB::Tx* impl;

	//! error code
	int ptnk_errno;

	//! buffer used in read operations (mostly value)
	ptnk::Buffer read_buf;

	//! buffer used in read operations (mostly key)
	ptnk::Buffer read_buf2;

	ptnk_tx(ptnk::DB::Tx* impl_)
	:	impl(impl_),
		ptnk_errno(0)
	{ /* NOP */}

	~ptnk_tx()
	{
		delete impl;
	}
};

struct ptnk_cur
{
	//! pointer to the C++ impl.
	ptnk::DB::Tx::cursor_t* impl;

	//! pointer to tx
	ptnk_tx* tx;

	//! buffer used in read operations (mostly value)
	ptnk::Buffer read_buf;

	//! buffer used in read operations (mostly key)
	ptnk::Buffer read_buf2;

	ptnk_cur(ptnk::DB::Tx::cursor_t* impl_, ptnk_tx* tx_)
	:	impl(impl_),
		tx(tx_),
		read_buf(4096),
		read_buf2(4096)
	{ /* NOP */	}

	~ptnk_cur()
	{
		ptnk::DB::Tx::curClose(impl);
	}
};

#define COMMON_CATCH_BLOCKS(handle) \
	catch(std::exception& e) \
	{ \
		handle->ptnk_errno = EINVAL; \
		std::cerr << __func__ << ": " << e.what() << std::endl; \
		return 0; \
	} \
	catch(...) \
	{ \
		handle->ptnk_errno = EINVAL; \
		std::cerr << __func__ << ": unknown exception caught" << std::endl; \
		return 0; \
	}

#define COMMON_CATCH_BLOCKS_DATUM(handle) \
	catch(std::exception& e) \
	{ \
		handle->ptnk_errno = EINVAL; \
		std::cerr << __func__ << ": " << e.what() << std::endl; \
		ptnk_datum ret = {NULL, PTNK_ERR_TAG}; \
		return ret; \
	} \
	catch(...) \
	{ \
		handle->ptnk_errno = EINVAL; \
		std::cerr << __func__ << ": unknown exception caught" << std::endl; \
		ptnk_datum ret = {NULL, PTNK_ERR_TAG}; \
		return ret; \
	}

#define COMMON_CATCH_BLOCKS_PTR(handle) \
	catch(std::exception& e) \
	{ \
		handle->ptnk_errno = EINVAL; \
		std::cerr << __func__ << ": " << e.what() << std::endl; \
		return NULL; \
	} \
	catch(...) \
	{ \
		handle->ptnk_errno = EINVAL; \
		std::cerr << __func__ << ": unknown exception caught" << std::endl; \
		return NULL; \
	}

inline
ptnk::BufferCRef
datum2CRef(ptnk_datum_t d)
{
	return ptnk::BufferCRef(d.dptr, d.dsize);
}

ptnk_db_t*
ptnk_open(const char* filename, ptnk_opts_t opts, int mode)
try
{
	LOG_OUTF("ptnk_open(filename = %s, opts = %d, mode = 0%o);\n", filename, opts, mode);

	return new ptnk_db_t(new ptnk::DB(filename, opts, mode));;
}
catch(std::exception& e)
{
	std::cerr << __func__ << ": " << e.what() << std::endl;
	return NULL;
}
catch(...)
{
	std::cerr << __func__ << ": unknown exception caught" << std::endl;;
	return NULL;
}

ptnk_db_t*
ptnk_open_dbm(const char* file, int flags, int mode)
{
	LOG_OUTF("ptnk_open_dbm(file = %s, flags = %d, mode = 0%o);\n", file, flags, mode);

	ptnk_opts_t opts = 0;
	
	if(flags & O_RDWR) opts |= ptnk::OWRITER;
	if(flags & O_CREAT) opts |= ptnk::OCREATE;
	if(flags & O_TRUNC) opts |= ptnk::OTRUNCATE;
	if(flags & O_SYNC) opts |= ptnk::OAUTOSYNC;

	return ptnk_open(file, opts, mode);
}

void
ptnk_close(ptnk_db_t* db)
{
	LOG_OUTF("ptnk_close(db = %p);\n", db);

	if(!db)
	{
		std::cerr << "ptnk_close: db is NULL" << std::endl;
		return;	
	}

	delete db;
}

int
ptnk_drop_db(const char* file)
try
{
	LOG_OUTF("ptnk_drop_db(file = %s);\n", file);

	ptnk::DB::drop(file);

	return 1;
}
catch(std::exception& e)
{
	std::cerr << __func__ << ": " << e.what() << std::endl;
	return 0;
}
catch(...)
{
	std::cerr << __func__ << ": unknown exception caught" << std::endl;
	return 0;
}

int
ptnk_put(ptnk_db_t* db, ptnk_datum_t key, ptnk_datum_t value, int mode)
try
{
	LOG_OUTF("ptnk_put(db = %p, key = {%p, %u}, value = {%p, %u}, mode = %d);\n", db, key.dptr, key.dsize, value.dptr, value.dsize, mode);

	PTNK_ASSERT(db->impl);

	db->impl->put(datum2CRef(key), datum2CRef(value), (ptnk::put_mode_t)mode);

	return 1;
}
catch(ptnk::ptnk_duplicate_key_error&)
{
	db->ptnk_errno = PTNK_EDUPKEY;
	return 0;
}
COMMON_CATCH_BLOCKS(db)

int
ptnk_put_cstr(ptnk_db_t* db, const char* key, const char* value, int mode)
try
{
	LOG_OUTF("ptnk_put_cstr(db = %p, key = %s, value = %s, mode = %d);\n", db, key, value, mode);

	ptnk_datum_t k = {const_cast<char*>(key), static_cast<int>(::strlen(key))};
	ptnk_datum_t v = {const_cast<char*>(value), static_cast<int>(::strlen(value))};

	return ptnk_put(db, k, v, (ptnk::put_mode_t)mode);
}
COMMON_CATCH_BLOCKS(db)

ptnk_datum_t
ptnk_get(ptnk_db_t* db, ptnk_datum_t key)
try
{
	LOG_OUTF("ptnk_get(db = %p, key = {%p, %u});\n", db, key.dptr, key.dsize);

	db->impl->get(datum2CRef(key), &db->read_buf);

	ptnk_datum_t ret = {db->read_buf.get(), static_cast<int>(db->read_buf.valsize())};
	return ret;
}
COMMON_CATCH_BLOCKS_DATUM(db)

const char*
ptnk_get_cstr(ptnk_db_t* db, const char* key)
try
{
	LOG_OUTF("ptnk_get_cstr(db = %p, key = %s);\n", db, key);

	db->impl->get(ptnk::cstr2ref(key), &db->read_buf);

	if(db->read_buf.valsize() >= 0)
	{
		db->read_buf.makeNullTerm();

		LOG_OUTF("ptnk_get ret: %s\n", db->read_buf.get());
		return db->read_buf.get();
	}
	else
	{
		LOG_OUTF("ptnk_get ret: (NULL)\n");
		return NULL;
	}
}
COMMON_CATCH_BLOCKS_PTR(db)

int
ptnk_error(ptnk_db_t* db)
{
	return db->ptnk_errno;
}

int
ptnk_clearerr(ptnk_db_t* db)
{
	db->ptnk_errno = 0;

	return 1;
}

ptnk_tx_t*
ptnk_tx_begin(ptnk_db_t* db)
try
{
	LOG_OUTF("ptnk_tx_begin(db = %p);\n", db);
	return new ptnk_tx_t(db->impl->newTransaction());
}
COMMON_CATCH_BLOCKS_PTR(db)

int
ptnk_tx_end(ptnk_tx_t* tx, int commit)
try
{
	LOG_OUTF("ptnk_tx_end(tx = %p, commit = %d);\n", tx, commit);
	std::auto_ptr<ptnk_tx_t> tx_(tx);

	if(commit)
	{
		return tx_->impl->tryCommit() ? 1 : 0;
	}
	else
	{
		// commit not tried at all
		return 0;
	}
}
COMMON_CATCH_BLOCKS(tx)

int
ptnk_tx_error(ptnk_tx_t* tx)
{
	return tx->ptnk_errno;
}

int
ptnk_tx_put(ptnk_tx_t* tx, ptnk_datum_t key, ptnk_datum_t value, int mode)
try
{
	LOG_OUTF("ptnk_tx_put(tx = %p, key = {%p, %u}, value = {%p, %u}, mode = %d);\n", tx, key.dptr, key.dsize, value.dptr, value.dsize, mode);
	PTNK_ASSERT(tx->impl);

	tx->impl->put(datum2CRef(key), datum2CRef(value), (ptnk::put_mode_t)mode);

	return 1;
}
catch(ptnk::ptnk_duplicate_key_error&)
{
	tx->ptnk_errno = PTNK_EDUPKEY;
	return 0;
}
COMMON_CATCH_BLOCKS(tx)

int
ptnk_tx_put_cstr(ptnk_tx_t* tx, const char* key, const char* value, int mode)
try
{
	LOG_OUTF("ptnk_tx_put_cstr(tx = %p, key = %s, value = %s, mode = %d);\n", tx, key, value, mode);
	PTNK_ASSERT(tx->impl);

	tx->impl->put(ptnk::cstr2ref(key), ptnk::cstr2ref(value), (ptnk::put_mode_t)mode);

	return 1;
}
catch(ptnk::ptnk_duplicate_key_error&)
{
	tx->ptnk_errno = PTNK_EDUPKEY;
	return 0;
}
COMMON_CATCH_BLOCKS(tx)

ptnk_datum_t
ptnk_tx_get(ptnk_tx_t* tx, ptnk_datum_t key)
try
{
	LOG_OUTF("ptnk_tx_get(tx = %p, key = {%p, %u});\n", tx, key.dptr, key.dsize);

	tx->impl->get(datum2CRef(key), &tx->read_buf);

	ptnk_datum_t ret = {tx->read_buf.get(), static_cast<int>(tx->read_buf.valsize())};
	return ret;
}
COMMON_CATCH_BLOCKS_DATUM(tx)

const char*
ptnk_tx_get_cstr(ptnk_tx_t* tx, const char* key)
try
{
	LOG_OUTF("ptnk_tx_get_cstr(tx = %p, key = %s);\n", tx, key);

	tx->impl->get(ptnk::cstr2ref(key), &tx->read_buf);

	if(tx->read_buf.valsize() >= 0)
	{
		tx->read_buf.makeNullTerm();
		return tx->read_buf.get();
	}
	else
	{
		return NULL;
	}
}
COMMON_CATCH_BLOCKS_PTR(tx)

ptnk_table_t*
ptnk_table_open(ptnk_datum_t tableid)
{
	LOG_OUTF("ptnk_table_open(tableid = {%p, %d});\n", tableid.dptr, tableid.dsize);
	return static_cast<ptnk_table_t*>(new ptnk::TableOffCache(datum2CRef(tableid)));
}

ptnk_table_t*
ptnk_table_open_cstr(const char* tableid)
{
	LOG_OUTF("ptnk_table_open_cstr(tableid = %s);\n", tableid);
	return static_cast<ptnk_table_t*>(new ptnk::TableOffCache(ptnk::cstr2ref(tableid)));
}

void
ptnk_table_close(ptnk_table_t* table)
{
	LOG_OUTF("ptnk_table_close(table = %p);\n", table);
	delete static_cast<ptnk::TableOffCache*>(table);
}

int
ptnk_tx_table_put(ptnk_tx_t* tx, ptnk_table_t* table, ptnk_datum_t key, ptnk_datum_t value, int mode)
try
{
	LOG_OUTF("ptnk_tx_put(tx = %p, table = %p, key = {%p, %u}, value = {%p, %u}, mode = %d);\n", tx, table, key.dptr, key.dsize, value.dptr, value.dsize, mode);
	PTNK_ASSERT(tx->impl);

	ptnk::TableOffCache* toc = static_cast<ptnk::TableOffCache*>(table);
	tx->impl->put(toc, datum2CRef(key), datum2CRef(value), (ptnk::put_mode_t)mode);

	return 1;
}
catch(ptnk::ptnk_duplicate_key_error&)
{
	tx->ptnk_errno = PTNK_EDUPKEY;
	return 0;
}
COMMON_CATCH_BLOCKS(tx)

int
ptnk_tx_table_put_cstr(ptnk_tx_t* tx, ptnk_table_t* table, const char* key, const char* value, int mode)
try
{
	LOG_OUTF("ptnk_tx_put_cstr(tx = %p, table = %p, key = %s, value = %s, mode = %d);\n", tx, table, key, value, mode);
	PTNK_ASSERT(tx->impl);

	ptnk::TableOffCache* toc = static_cast<ptnk::TableOffCache*>(table);
	tx->impl->put(toc, ptnk::cstr2ref(key), ptnk::cstr2ref(value), (ptnk::put_mode_t)mode);

	return 1;
}
catch(ptnk::ptnk_duplicate_key_error&)
{
	tx->ptnk_errno = PTNK_EDUPKEY;
	return 0;
}
COMMON_CATCH_BLOCKS(tx)

ptnk_datum_t
ptnk_tx_table_get(ptnk_tx_t* tx, ptnk_table_t* table, ptnk_datum_t key)
try
{
	LOG_OUTF("ptnk_tx_table_get(tx = %p, table = %p, key = {%p, %u});\n", tx, table, key.dptr, key.dsize);

	ptnk::TableOffCache* toc = static_cast<ptnk::TableOffCache*>(table);
	tx->impl->get(toc, datum2CRef(key), &tx->read_buf);

	ptnk_datum_t ret = {tx->read_buf.get(), static_cast<int>(tx->read_buf.valsize())};
	return ret;
}
COMMON_CATCH_BLOCKS_DATUM(tx)

const char*
ptnk_tx_table_get_cstr(ptnk_tx_t* tx, ptnk_table_t* table, const char* key)
try
{
	LOG_OUTF("ptnk_tx_table_get_cstr(tx = %p, key = %s);\n", tx, key);

	ptnk::TableOffCache* toc = static_cast<ptnk::TableOffCache*>(table);
	tx->impl->get(toc, ptnk::cstr2ref(key), &tx->read_buf);

	if(tx->read_buf.valsize() >= 0)
	{
		tx->read_buf.makeNullTerm();
		return tx->read_buf.get();
	}
	else
	{
		return NULL;
	}
}
COMMON_CATCH_BLOCKS_PTR(tx)

ptnk_cur_t*
ptnk_cur_front(ptnk_tx_t* tx, ptnk_table_t* table)
try
{
	LOG_OUTF("ptnk_cur_front(tx = %p, table = %p);\n", tx, table);
	
	ptnk::TableOffCache* toc = static_cast<ptnk::TableOffCache*>(table);
	ptnk::DB::Tx::cursor_t* ptnkcur = tx->impl->curFront(toc);

	if(ptnkcur)
	{
		return new ptnk_cur_t(ptnkcur, tx);
	}
	else
	{
		return NULL;	
	}
}
COMMON_CATCH_BLOCKS_PTR(tx)

ptnk_cur_t*
ptnk_query(ptnk_tx_t* tx, ptnk_table_t* table, ptnk_datum_t key, int query_type)
try
{
	LOG_OUTF("ptnk_query(tx = %p, table = %p, key = {%p, %d}, query_type = %d);\n", tx, table, key.dptr, key.dsize, query_type);
	
	ptnk::query_t q = {datum2CRef(key), (ptnk::query_type_t)query_type};
	
	ptnk::TableOffCache* toc = static_cast<ptnk::TableOffCache*>(table);
	ptnk::DB::Tx::cursor_t* ptnkcur = tx->impl->curQuery(toc, q);

	if(ptnkcur)
	{
		return new ptnk_cur_t(ptnkcur, tx);
	}
	else
	{
		return NULL;	
	}
}
COMMON_CATCH_BLOCKS_PTR(tx)

int
ptnk_cur_next(ptnk_cur_t* cur)
try
{
	LOG_OUTF("ptnk_cur_next(cur = %p);\n", cur);

	if(cur->tx->impl->curNext(cur->impl))
	{
		return 1;	
	}
	else
	{
		// no more next record
		return 0;	
	}
}
COMMON_CATCH_BLOCKS(cur->tx)

int
ptnk_cur_get(ptnk_datum_t* key, ptnk_datum_t* value, ptnk_cur_t* cur)
try
{
	LOG_OUTF("ptnk_cur_get(key = %p, value = %p, cur = %p);\n", key, value, cur);

	cur->tx->impl->curGet(&cur->read_buf2, &cur->read_buf, cur->impl);

	key->dptr = cur->read_buf2.get();
	key->dsize = cur->read_buf2.valsize();

	value->dptr = cur->read_buf.get();
	value->dsize = cur->read_buf.valsize();

	return 1;
}
COMMON_CATCH_BLOCKS(cur->tx)

int
ptnk_cur_get_cstr(const char** key, const char** value, ptnk_cur_t* cur)
try
{
	LOG_OUTF("ptnk_cur_get(key = %p, value = %p, cur = %p);\n", key, value, cur);

	cur->tx->impl->curGet(&cur->read_buf2, &cur->read_buf, cur->impl);

	// FIXME: handle null cases?

	cur->read_buf.makeNullTerm();
	cur->read_buf2.makeNullTerm();

	*key = cur->read_buf2.get();
	*value = cur->read_buf.get();

	return 1;
}
COMMON_CATCH_BLOCKS(cur->tx)

int
ptnk_cur_put(ptnk_cur_t* cur, ptnk_datum_t value)
try
{
	LOG_OUTF("ptnk_cur_put(cur = %p, value = {%p, %d});\n", cur, value.dptr, value.dsize);

	cur->tx->impl->curPut(cur->impl, datum2CRef(value));

	return 1;
}
COMMON_CATCH_BLOCKS(cur->tx);

int
ptnk_cur_delete(ptnk_cur_t* cur)
try
{
	LOG_OUTF("ptnk_cur_delete(cur = %p);\n", cur);

	if(cur->tx->impl->curDelete(cur->impl))
	{
		return 1;	
	}
	else
	{
		return 0;	
	}

	return 1;
}
COMMON_CATCH_BLOCKS(cur->tx);

void ptnk_cur_close(ptnk_cur_t* cur)
{
	if(cur) delete cur;
}

int
ptnk_tx_table_create(ptnk_tx_t* tx, ptnk_datum_t table)
try
{
	LOG_OUTF("ptnk_table_create(tx = %p, table = {%p, %u});\n", tx, table.dptr, table.dsize);
	tx->impl->tableCreate(datum2CRef(table));

	return 1;
}
COMMON_CATCH_BLOCKS(tx)

int
ptnk_tx_table_create_cstr(ptnk_tx_t* tx, const char* table)
try
{
	LOG_OUTF("ptnk_table_create_cstr(tx = %p, table = %s);\n", tx, table);
	tx->impl->tableCreate(ptnk::cstr2ref(table));

	return 1;
}
COMMON_CATCH_BLOCKS(tx)

int
ptnk_tx_table_drop(ptnk_tx_t* tx, ptnk_datum_t table)
try
{
	LOG_OUTF("ptnk_table_drop(tx = %p, table = {%p, %u});\n", tx, table.dptr, table.dsize);
	tx->impl->tableDrop(datum2CRef(table));
	
	return 1;
}
COMMON_CATCH_BLOCKS(tx)

int
ptnk_tx_table_drop_cstr(ptnk_tx_t* tx, const char* table)
try
{
	LOG_OUTF("ptnk_table_drop_cstr(tx = %p, table = %s);\n", tx, table);
	tx->impl->tableDrop(ptnk::cstr2ref(table));

	return 1;
}
COMMON_CATCH_BLOCKS(tx)

const char*
ptnk_tx_table_get_name_cstr(ptnk_tx_t* tx, int idx)
try
{
	LOG_OUTF("ptnk_tx_table_get_name_cstr(tx = %p, idx = %d);\n", tx, idx);

	tx->impl->tableGetName(idx, &tx->read_buf);
	tx->read_buf.makeNullTerm();

	if(tx->read_buf.valsize() >= 0)
	{
		tx->read_buf.makeNullTerm();

		LOG_OUTF("ptnk_tx_table_get_name_cstr ret: %s\n", tx->read_buf.get());
		return tx->read_buf.get();
	}
	else
	{
		LOG_OUTF("ptnk_tx_table_get_name_cstr ret: (NULL)\n");
		return NULL;
	}
}
COMMON_CATCH_BLOCKS_PTR(tx)
