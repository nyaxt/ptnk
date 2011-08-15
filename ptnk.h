#ifndef _ptnk_h_
#define _ptnk_h_

/*!
 * @file ptnk.h
 * @brief basic definitions of ptnk db library C API
 */

#include "ptnk/types.h"

#ifdef __cplusplus
#include "ptnk/db.h"
#endif /* __cplusplus */

#ifndef PTNK_NO_CAPI

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

struct ptnk_db;
typedef struct ptnk_db ptnk_db_t;
typedef struct ptnk_tx ptnk_tx_t;
typedef void ptnk_table_t;
typedef struct ptnk_cur ptnk_cur_t;

struct ptnk_datum
{
	char* dptr;
	int dsize;
};
typedef struct ptnk_datum ptnk_datum_t;

/* ptnk C apis */

/* ndbm compatible apis */

/*! create new / open existing database and return its handle */
/*!
 *  @param[in] file		path to db file
 *  @param[in] opts		ptnk options
 *  @param[in] mode		db file creat(2) mode
 *
 *  @return new db handle
 */
ptnk_db_t* ptnk_open(const char* file, ptnk_opts_t opts, int mode);

/*! create new / open existing database and return its handle (ndbm style) */
/*!
 *	@param[in] file		path to db file
 *	@param[in] flags	file open flags ::open(2) style (O_RDWR, O_CREAT, O_TRUNC, O_SYNC) are valid
 *	@param[in] mode		create file mode (valid when flags&O_CREAT)
 *
 *  @return new db handle
 */
ptnk_db_t* ptnk_open_dbm(const char* file, int flags, int mode);

/*! close opened db handle */
/*! 
 *  @param[in] db	opened db handle
 */
void ptnk_close(ptnk_db_t* db);

/*! insert new / update existing record */
/*! 
 *  @param[in] db		opened db handle
 *  @param[in] key		record key
 *  @param[in] data		record value
 *  @param[in] mode		type of store operation (PUT_INSERT | PUT_UPDATE)
 *
 *  @return return non-zero on success
 */
int ptnk_put(ptnk_db_t* db, ptnk_datum_t key, ptnk_datum_t value, int mode);

/*! insert new / update existing string record */
/*! 
 *  @param[in] db		opened db handle
 *  @param[in] key		record key in null-terminated string
 *  @param[in] data		record value in null-terminated string
 *  @param[in] mode		type of store operation (PUT_INSERT | PUT_UPDATE)
 *
 *  @return return non-zero on success
 */
int ptnk_put_cstr(ptnk_db_t* db, const char* key, const char* value, int mode);

/*! fetch stored record */
/*!
 *	@param[in] db		opened db handle
 *	@param[in] key		record key
 *
 *	@caution
 *		This function is not multithread safe. The fetched record is stored in internal buffer stored inside ptnk_db_t.
 *
 *	@return fetched record
 */
ptnk_datum_t ptnk_get(ptnk_db_t* db, ptnk_datum_t key);

/*! fetch stored string record */
/*! 
 *  @param[in] db		opened db handle
 *  @param[in] key		record key in null-terminated string
 *
 *  @return
 *		fetched record value string stored in ptnk_db_t internal buffer on success.
 *		return NULL when the record is not found
 */
const char* ptnk_get_cstr(ptnk_db_t* db, const char* key);

int ptnk_delete(ptnk_db_t* db, ptnk_datum_t key);

ptnk_datum_t ptnk_firstkey(ptnk_db_t* db);
ptnk_datum_t ptnk_nextkey(ptnk_db_t* db);

/*! duplicate key found */
#define PTNK_EDUPKEY 10000

int ptnk_error(ptnk_db_t* db);
int ptnk_clearerr(ptnk_db_t* db);

/*! begin transaction */
/*!
 *	@return new transaction handle
 */
ptnk_tx_t* ptnk_tx_begin(ptnk_db_t* db);

#define PTNK_TX_ABORT	0
#define PTNK_TX_COMMIT	1

/*! end transaction */
/*!
 *	@param[in] tx		transaction handle
 *  @param[in] commit	if non-zero, try to commit the transaction
 *
 *  @return return non-zero if the transaction was successfully committed
 */
int ptnk_tx_end(ptnk_tx_t* tx, int commit);

/*! get last error code for tx related op. */
int ptnk_tx_error(ptnk_tx_t* tx);

/*! insert new / update existing record within transaction */
/*! 
 *  @param [in] tx		opened transaction handle
 *  @param [in] key		record key
 *  @param [in] data	record value
 *  @param [in] mode	type of store operation (PUT_INSERT | PUT_UPDATE)
 *
 *  @return return non-zero on success
 */
int ptnk_tx_put(ptnk_tx_t* tx, ptnk_datum_t key, ptnk_datum_t value, int mode);

/*! insert new / update existing string record within transaction */
/*! 
 *  @param [in] tx		opened transaction handle
 *  @param [in] key		record key
 *  @param [in] data	record value
 *  @param [in] mode	type of store operation (PUT_INSERT | PUT_UPDATE)
 *
 *  @return return non-zero on success
 */
int ptnk_tx_put_cstr(ptnk_tx_t* tx, const char* key, const char* value, int mode);

/*! fetch stored record from snapshot stored in _tx_ */
/*!
 *  @param [in] tx		opened transaction handle
 *	@param [in] key		record key
 *
 *	@return fetched record
 */
ptnk_datum_t ptnk_tx_get(ptnk_tx_t* tx, ptnk_datum_t key);

/*! fetch stored string record from snapshot stored in _tx_ */
/*! 
 *  @param [in] tx		opened transaction handle
 *  @param [in] key		record key in null-terminated string
 *
 *  @return
 *		fetched record value string stored in ptnk_tx_t internal buffer on success.
 *		return NULL when the record is not found
 */
const char* ptnk_tx_get_cstr(ptnk_tx_t* tx, const char* key);

/*! create table offset cache for table _tableid_ */
/*!
 *	@param [in]	tableid	table identifier
 *
 *	@return
 *		table offset cache for table identified by _tableid_
 */
ptnk_table_t* ptnk_table_open(ptnk_datum_t tableid);

/*! create table offset cache for table _tableid_ */
/*!
 *	@param [in]	tableid	table identifier in null-terminated string
 *
 *	@return
 *		table offset cache for table identified by _tableid_
 */
ptnk_table_t* ptnk_table_open_cstr(const char* tableid);

/*! delete table offset cache _table_ */
/*!
 *	@param [in] table
 */
void ptnk_table_close(ptnk_table_t* table);

/*! insert new / update existing record within transaction */
/*! 
 *  @param [in] tx			opened transaction handle
 *  @param [in,out] table	table offset cache
 *  @param [in] key			record key
 *  @param [in] data		record value
 *  @param [in] mode		type of store operation (PUT_INSERT | PUT_UPDATE)
 *
 *  @return return non-zero on success
 */
int ptnk_tx_table_put(ptnk_tx_t* tx, ptnk_table_t* table, ptnk_datum_t key, ptnk_datum_t value, int mode);

/*! insert new / update existing string record within transaction */
/*! 
 *  @param [in] tx			opened transaction handle
 *  @param [in,out] table	table offset cache
 *  @param [in] key			record key
 *  @param [in] data		record value
 *  @param [in] mode		type of store operation (PUT_INSERT | PUT_UPDATE)
 *
 *  @return return non-zero on success
 */
int ptnk_tx_table_put_cstr(ptnk_tx_t* tx, ptnk_table_t* table, const char* key, const char* value, int mode);

/*! fetch stored record from snapshot stored in _tx_ */
/*!
 *  @param [in] tx			opened transaction handle
 *  @param [in,out] table	table offset cache
 *	@param [in] key			record key
 *
 *	@return fetched record
 */
ptnk_datum_t ptnk_tx_table_get(ptnk_tx_t* tx, ptnk_table_t* table, ptnk_datum_t key);

/*! fetch stored string record from snapshot stored in _tx_ */
/*! 
 *  @param [in] tx		opened transaction handle
 *  @param [in,out] table	table offset cache
 *  @param [in] key		record key in null-terminated string
 *
 *  @return
 *		fetched record value string stored in ptnk_tx_t internal buffer on success.
 *		return NULL when the record is not found
 */
const char* ptnk_tx_table_get_cstr(ptnk_tx_t* tx, ptnk_table_t* table, const char* key);

/*! get cursor pointing to the first record in the table */
/*!
 *	@param [in] tx		transaction handle. This is required as cursor operations are applied to snapshot specified in the transaction.
 *	@param [in] table	target table's offset cache
 *
 *	@return
 *		new cursor pointing to the first record in the table
 *		OR NULL if there are no entry in the table
 */
ptnk_cur_t* ptnk_cur_front(ptnk_tx_t* tx, ptnk_table_t* table);

/*! query table and create new cursor pointing to the result record */
/*!
 *	@param [in] tx		transaction handle. This is required as cursor operations are applied to snapshot specified in the transaction.
 *	@param [in] table	target table's offset cache
 *	@param [in] key		query target key
 *	@param [in] query_type	query type (see enum query_type_t)
 *
 *	@return
 *		
 */
ptnk_cur_t* ptnk_query(ptnk_tx_t* tx, ptnk_table_t* table, ptnk_datum_t key, int query_type);

/*! fetch record pointed by the cursor */
/*!
 *	@param [out] key	buffer where fetched key data is stored
 *	@param [out] value	buffer where fetched value data is stored
 *	@param [in] cur		cursor pointing to a record we want to fetch
 */
int ptnk_cur_get(ptnk_datum_t* key, ptnk_datum_t* value, ptnk_cur_t* cur);

/*! fetch null-terminated string record pointed by the cursor */
/*!
 *	@param [out] key	cstr ptr to fetched key data is stored
 *	@param [out] value	cstr ptr to fetched value data is stored
 *	@param [in] cur		cursor pointing to a record we want to fetch
 */
int ptnk_cur_get_cstr(const char** key, const char** value, ptnk_cur_t* cur);

/*! change value of the record pointed by the cursor */
/*!
 *	@param [in,out] cur	cursor pointing to a record we want to fetch. This would be updated to point to modified record.
 *	@param [in] value	buffer where fetched value data is stored
 */
int ptnk_cur_put(ptnk_cur_t* cur, ptnk_datum_t value);

/*! delete the record pointed by the cursor and make cursor point to next valid record */
/*!
 *  @param [in,out] cur	cursor pointing to a record we want to delete. This would be updated to point to next valid record
 *  
 *  @return
 *		non-zero if cursor is still valid, zero if cursor is invalid
 */
int ptnk_cur_delete(ptnk_cur_t* cur);

/*! make cursor point to the next record */
/*!
 *	@param [in,out] cur	valid cursor handle
 */
int ptnk_cur_next(ptnk_cur_t* cur);

/*! close opened cursor */
/*!
 *	@param [in] cur		cursor to close
 *
 *	Any opened cursor by ptnk_query/ptnk_cur_front/ptnk_cur_back functions must be closed by this function after use.
 */
void ptnk_cur_close(ptnk_cur_t* cur);

/*! create table with identifier _table_ */
/*!
 *  @param [in] table	table identifier of the newly created table
 *
 *  @return non-zero on success
 */
int ptnk_tx_table_create(ptnk_tx_t* tx, ptnk_datum_t table);

/*! create table with identifier _table_ */
/*!
 *  @param [in] table	table identifier in null-terminated string
 *
 *  @return non-zero on success
 */
int ptnk_tx_table_create_cstr(ptnk_tx_t* tx, const char* table);

/*! delete table with identifier _table_ */
/*!
 *	@param [in] table	table identifier of the table to delete
 *
 *	@return non-zero on success
 */
int ptnk_tx_table_drop(ptnk_tx_t* tx, ptnk_datum_t table);

/*! delete table with identifier _table_ */
/*!
 *	@param [in] table	table identifier in null-terminated string
 *
 *	@return non-zero on success
 */
int ptnk_tx_table_drop_cstr(ptnk_tx_t* tx, const char* table);

#ifdef PTNK_NDBM_COMPAT
/* source code compatibility w/ ndbm */

#define DBM ptnk_db_t
#define datum ptnk_datum_t

#ifdef PTNK_ADD_PREFIX
#define P_(x) PTNK_##x
#else
#define P_(x) x
#endif

#define DBM_INSERT P_(PUT_INSERT)
#define DBM_REPLACE P_(PUT_REPLACE)

#undef P_

#define dbm_open ptnk_open_dbm
#define dbm_close ptnk_close
#define dbm_store ptnk_put
#define dbm_fetch ptnk_get
#define dbm_delete ptnk_delete
#define dbm_firstkey ptnk_firstkey
#define dbm_nextkey ptnk_nextkey
#define dbm_error ptnk_error
#define dbm_clearerr ptnk_clearerr

#endif /* PTNK_NDBM_COMPAT */

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* PTNK_NO_CAPI */

#endif /* _ptnk_h_ */
