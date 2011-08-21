#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include <pthread.h>
#include <stdio.h>
#include <iostream>
#include <string>

// #define MYPTNK_DEBUG

#ifdef MYPTNK_DEBUG
#define DEBUG_OUTF(...) do { fprintf(stdout, "ha_myptnk: " __VA_ARGS__); fflush(stdout);} while (0)
#define WARN_STUB do { DEBUG_OUTF("%p STUB FUNCTION %s called!\n", this, __FUNCTION__); } while (0)
#else
#define DEBUG_OUTF(...) 
#define WARN_STUB
#endif

#include "sql_priv.h"
#include "sql_class.h"
#include "ha_myptnk.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

class mutex_wrap
{
public:
	mutex_wrap()
	{
		pthread_mutex_init(&m_impl, NULL);	
	}

	void lock() { pthread_mutex_lock(&m_impl); }
	void unlock() { pthread_mutex_unlock(&m_impl); }

	class guard
	{
	public:
		guard(mutex_wrap& mtx)
		: m_mtx(mtx)
		{ m_mtx.lock(); }

		~guard()
		{ m_mtx.unlock(); }

	private:
		mutex_wrap& m_mtx;
	};

private:
	pthread_mutex_t m_impl;
};

struct myptnk_txn
{
	ptnk_tx_t* ptnktx;
	bool readonly;
	int count;

	myptnk_txn(ptnk_db_t* ptnkdb, bool readonly_)
	:	ptnktx(::ptnk_tx_begin(ptnkdb)),
		readonly(readonly_),
		count(1)
	{
		/* NOP */	
	}

	~myptnk_txn()
	{
		if(ptnktx)
		{
			::ptnk_tx_end(ptnktx, PTNK_TX_ABORT);	
		}
	}

	void incCount()
	{
		++ count;	
	}

	bool decCount()
	{
		return (--count == 0);
	}
};

// static myptnk_share_map_t g_myptnk_open_tables;
static mutex_wrap g_myptnk_giant_mtx;

static handler* myptnk_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
	return new (mem_root) ha_myptnk(hton, table);
}

static 
int
myptnk_commit(handlerton* hton, THD *thd, bool all)
{
	int errcode = 0;

	DEBUG_OUTF("myptnk_commit. thd: %p all: %d\n", thd, all);

	myptnk_txn* txn = static_cast<myptnk_txn*>(thd_get_ha_data(thd, hton));
	if(! txn)
	{
		// no on-going txn
		return 0;	
	}

	if(! txn->readonly)
	{
		// not read-only txn...
		
		// try committing changes
		if(! ::ptnk_tx_end(txn->ptnktx, PTNK_TX_COMMIT))
		{
			errcode = HA_ERR_RECORD_CHANGED;	
		}
		txn->ptnktx = NULL;
	}

	delete txn;

	txn = NULL;
	thd_set_ha_data(thd, hton, txn);

	return errcode;
}

static
int
myptnk_rollback(handlerton* hton, THD* thd, bool all)
{
	DEBUG_OUTF("myptnk_rollback. thd: %p all: %d\n", thd, all);

	myptnk_txn* txn = static_cast<myptnk_txn*>(thd_get_ha_data(thd, hton));
	if(! txn)
	{
		// no on-going txn
		return 0;	
	}

	// don't perform commit
	delete txn;

	txn = NULL;
	thd_set_ha_data(thd, hton, txn);

	return 0;
}

static
int
myptnk_init_func(void *p)
{
	DBUG_ENTER("myptnk_init_func");

	// init mutex here

	handlerton* myptnk_hton = (handlerton *)p;
	myptnk_hton->state = SHOW_OPTION_YES;
	myptnk_hton->create = myptnk_create_handler;
	myptnk_hton->commit = myptnk_commit;
	myptnk_hton->rollback = myptnk_rollback;
	myptnk_hton->flags = HTON_ALTER_NOT_SUPPORTED | HTON_CAN_RECREATE;

	DBUG_RETURN(0);
}

static
int
myptnk_done_func(void *p)
{
	DBUG_ENTER("myptnk_done_func");

	mutex_wrap::guard g(g_myptnk_giant_mtx);
	/*
	if(! g_myptnk_open_tables.empty())
	{
		// found open tables
		DBUG_RETURN(1);	
	}
	*/

	DBUG_RETURN(0);
}

struct myptnk_share
{
	int useCount;
	char* dbname;
	THR_LOCK mysql_lock;
	mutex_wrap mtx;

	ptnk_db_t* ptnkdb;

	myptnk_share(char* dbname)
	:	useCount(0),
		dbname(dbname),
		ptnkdb(NULL)
	{
		thr_lock_init(&mysql_lock);
	}

	~myptnk_share()
	{
		if(ptnkdb)
		{
			::ptnk_close(ptnkdb);
			ptnkdb = NULL;
		}

		::free(dbname);
		thr_lock_delete(&mysql_lock);
	}

	void dbfilepath(char* path)
	{
		snprintf(path, sizeof path, "%s/%s", dbname, dbname);

		DEBUG_OUTF("path: %s\n", path);
	}

	int open(int mode)
	{
		if(ptnkdb)
		{
			DEBUG_OUTF("ptnkdb already opened\n");
			return 0;
		}

		char path[512]; dbfilepath(path);

		if(! (ptnkdb = ::ptnk_open(path, PTNK_OWRITER | PTNK_OCREATE | PTNK_OAUTOSYNC | PTNK_OPARTITIONED, 0644)))
		{
			return ER_CANT_OPEN_FILE;
		}
	}

	int dropdb()
	{
		char path[512]; dbfilepath(path);
		
		if(! ::ptnk_drop_db(path))
		{
			return ER_CANT_OPEN_FILE;
		}

		return 0;
	}
};

static
myptnk_share*
get_share(const char *db_table_name, TABLE *table)
{
	mutex_wrap::guard g(g_myptnk_giant_mtx);

	// skip ./
	if(db_table_name[0] == '.') db_table_name += 2;

	DEBUG_OUTF("get_share: db+table name: %s\n", db_table_name);

	// get db name
	size_t len = ::strlen(db_table_name);
	char* dbname = (char*)::malloc(len + 1); ::strcpy(dbname, db_table_name);
	for(int i = 0; i < len; ++ i) 
	{
		if(dbname[i] == '/')
		{
			dbname[i] = '\0';
			break;	
		}
	}
	DEBUG_OUTF("get_share: dbname: %s\n", dbname);

	// find share of that name
	// FIXME! FIXME!
	static myptnk_share* share = 0;
	if(! share)
	{
		share = new myptnk_share(dbname);
	}
	else
	{
		::free(dbname);
	}

	return share;	
}

static
int
free_share(myptnk_share* table)
{
	mutex_wrap::guard g(g_myptnk_giant_mtx);
	
	#if 0
	if (! --table->useCount)
	{
		g_myptnk_open_tables.erase(table->strName);
	}
	#endif

	return 0;
}

ha_myptnk::ha_myptnk(handlerton *hton, TABLE_SHARE *table_arg)
:	handler(hton, table_arg),
	m_table_share(NULL),
	m_txn(0),
	m_table_name(NULL)
{
	/* NOP */
}

size_t
ha_myptnk::calc_packed_rowsize(uchar* buf)
{
	size_t ret = 0;

	ret += table->s->reclength; // reclength == unpacked (on memory) row length
	ret += table->s->fields * 2; // allocate 2 bytes per field for storing field w/ variable lwength

	// for all blob fields ...
	for(int i = 0, n = table->s->blob_fields; i < n; ++ i)
	{
		Field_blob* field = reinterpret_cast<Field_blob*>(table->field[table->s->blob_field[i]]);

		ret += 2; // large blob may use 4 bytes to store length, so allocate another 2 bytes
		ret += field->get_length(); // blob size is not included in reclength as its stored as ptr
	}

	return ret;
}

void
ha_myptnk::copy_table_name(const char* name)
{
	DEBUG_OUTF("copy_table_name: name: %s, m_table_name: %s\n", name, m_table_name);

	if(m_table_name)
	{
		::free(m_table_name);	
	}

	// skip ./
	if(name[0] == '.') name += 2;

	// copy table name
	size_t len_name = ::strlen(name);
	DEBUG_OUTF("len_name: %u\n", len_name);
	const char* table_name = "default";
	for(int i = 0; i < len_name; ++ i)
	{
		if(name[i] == '/')
		{
			table_name = &name[i+1];
			break;
		}
	}

	DEBUG_OUTF("table_name: %s\n", table_name);
	m_table_name = (char*)::malloc(::strlen(table_name) + 1);
	::strcpy(m_table_name, table_name);

	DEBUG_OUTF("table name: %s\n", m_table_name);
}

const char** ha_myptnk::bas_ext() const
{
	static const char *ha_myptnk_exts[] = { ".ptnk", NullS };
	return ha_myptnk_exts;
}

int
ha_myptnk::open(const char *name, int mode, uint test_if_locked)
{
	DBUG_ENTER("ha_myptnk::open");

	WARN_STUB;

	if (!(m_table_share = get_share(name, table))) DBUG_RETURN(1);
	thr_lock_data_init(&m_table_share->mysql_lock, &lock, NULL);

	copy_table_name(name);
	m_ptnktable = ::ptnk_table_open_cstr(m_table_name);
	for(int i = 0; i < 8; ++ i) m_ptnktable_sidx[i] = NULL;
	for(int i = 1; i < table->s->keys; ++ i)
	{
		char idx_table_name[1024];
		sprintf(idx_table_name, "%s/%d", m_table_name, i);

		m_ptnktable_sidx[i-1] = ::ptnk_table_open_cstr(idx_table_name);
	}

	m_txn = NULL;

	m_cur = NULL;
	m_bSecondaryKeyActive = false;
	m_bSkipCurNext = true;

	DBUG_RETURN(m_table_share->open(mode));
}

int
ha_myptnk::close(void)
{
	DBUG_ENTER("ha_myptnk::close");

	if(m_cur) rnd_end();

	if(m_ptnktable)
	{
		::ptnk_table_close(m_ptnktable);
		m_ptnktable = NULL;
	}

	for(int i = 0; i < 8; ++ i)
	{
		if(m_ptnktable_sidx[i])
		{
			::ptnk_table_close(m_ptnktable_sidx[i]);
			m_ptnktable_sidx[i] = NULL;
		}
	}

	if(m_table_name)
	{
		::free(m_table_name);
		m_table_name = NULL;
	}

	if(m_table_share)
	{
		free_share(m_table_share);
		m_table_share = NULL;
	}

	DBUG_RETURN(free_share(m_table_share));
}

size_t
ha_myptnk::pack_key_from_mysqlrow(KEY* key, uchar *buf, char* dest)
{
	size_t lenKey = 0;

	if(table->s->keys > 0)
	{
		// a key consists of multiple key_parts
		//   ex) KEY `idx2` (`c2`,`c3`)
		//       key idx2 consists of multiple key_parts c2 and c3.

		char* p = dest;
		
		// pack each key parts
		const int nkey_parts = key->key_parts;
		DEBUG_OUTF("key name: %s key_parts: %d\n", key->name, key->key_parts);
		for(int i = 0; i < nkey_parts; ++ i)
		{
			KEY_PART_INFO* key_part	= &key->key_part[i];
			if(key_part->null_bit)
			{
				if(buf[key_part->null_offset])
				{
					*p++ = 0;
				}
				else
				{
					*p++ = 1;
				}
			}

			// memo: key_part->field can't be used here AND key_part->field != table->field[key_part->fieldnr-1]
			//       its field->table->s is nil, causing SEGV in pack member func. 
			Field* field = table->field[key_part->fieldnr - 1 /* offset 1 */];
			// p = (char*)field->pack((uchar*)p, buf + field->offset(buf));
			switch(field->type())
			{
			case MYSQL_TYPE_LONG:
				*(unsigned long*)p = htonl(*(unsigned long*)(buf + field->offset(buf))); p += 4;
				break;

			case MYSQL_TYPE_STRING:
			case MYSQL_TYPE_VARCHAR:
				{
					unsigned short len = field->data_length();
					int len_bytes = len > 255 ? 2 : 1;
					*(unsigned short*)p = len; p += 2;
					::memcpy(p, buf + field->offset(buf) + len_bytes, len); p += len;
				}
				break;

			default:
				DEBUG_OUTF("unknown key type: %d\n", key_part->field->type());
				// use default packer
				p = (char*)field->pack((uchar*)p, buf + field->offset(buf));	
				break;
			}
		}

		lenKey = p - dest;
	}
	DEBUG_OUTF("packed key len: %lu\n", lenKey);
	DEBUG_OUTF("packed key dump %02x %02x %02x %02x %02x\n", dest[0], dest[1], dest[2], dest[3], dest[4]);

	return lenKey;
}

int
ha_myptnk::write_row(uchar *buf)
{
	int rc = 0;

	// handle INSERTs
	DBUG_ENTER("ha_myptnk::write_row");

	// no active transaction
	if(! m_txn)
	{
		DEBUG_OUTF("error: no active transaction found");
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if(table->s->keys == 0)
	{
		// table w/ no key

		size_t bufsize = calc_packed_rowsize(buf);
		uchar* bufValue = reinterpret_cast<uchar*>(::malloc(bufsize));
		uchar* p = bufValue;
		DEBUG_OUTF("write_row alloced buf %lu\n", bufsize);

		// copy bitmap for null fields (null_bytes)
		::memcpy(p, buf, table->s->null_bytes);
		p += table->s->null_bytes;

		// pack each field
		const int nfields = table->s->fields;
		for(int i = 0; i < nfields; ++ i)
		{
			Field* field = table->field[i];	
			
			p = field->pack(p, buf + field->offset(buf));
		}
		DEBUG_OUTF("real row size: %lu\n", p - bufValue);

		ptnk_datum_t value_data = {(char*)bufValue, p - bufValue};
		DEBUG_OUTF("new value dump %02x %02x %02x %02x %02x\n", bufValue[0], bufValue[1], bufValue[2], bufValue[3], bufValue[4]);
		static ptnk_datum_t null_key = {NULL, 0};
		if(! ptnk_tx_table_put(m_txn->ptnktx, m_ptnktable, null_key, value_data, PTNK_PUT_INSERT))
		{
			DEBUG_OUTF("ptnk_tx_put has failed");
			rc = HA_ERR_INTERNAL_ERROR;	
		}

		::free(bufValue);

		DBUG_RETURN(rc);
	}

	// pack key
	DEBUG_OUTF("num keys: %d, key_parts: %d\n", table->s->keys, table->s->key_parts);
	DEBUG_OUTF("primary key idx: %d\n", table->s->primary_key);

	// -- primary key
	KEY* keyP = &table->s->key_info[table->s->primary_key];
	
	char bufKeyP[256];
	size_t sizeKeyP = pack_key_from_mysqlrow(keyP, buf, bufKeyP);

	ptnk_datum_t datumKeyP = {bufKeyP, sizeKeyP};

	// -- secondary keys
	int nKeys = table->s->keys;
	for(int i = 1; i < nKeys; ++ i)
	{
		KEY* key = &table->key_info[i];
		char bufKeyS[256];
		size_t sizeKeyS = pack_key_from_mysqlrow(key, buf, bufKeyS);
		ptnk_datum_t datumKeyS = {bufKeyS, sizeKeyS};

		if(! ptnk_tx_table_put(m_txn->ptnktx, m_ptnktable_sidx[i-1], datumKeyS, datumKeyP, PTNK_PUT_INSERT))
		{
			DEBUG_OUTF("ptnk_tx_put has failed for secondary key %d", i);
			rc = HA_ERR_INTERNAL_ERROR;	
		}
	}

	size_t bufsize = calc_packed_rowsize(buf);

	uchar* bufValue = reinterpret_cast<uchar*>(::malloc(bufsize));
	uchar* p = bufValue;
	DEBUG_OUTF("write_row alloced buf %lu\n", bufsize);

	// copy bitmap for null fields (null_bytes)
	::memcpy(p, buf, table->s->null_bytes);
	p += table->s->null_bytes;

	// pack each field
	const int nfields = table->s->fields;
	for(int i = 0; i < nfields; ++ i)
	{
		Field* field = table->field[i];	
		
		p = field->pack(p, buf + field->offset(buf));
	}
	DEBUG_OUTF("real row size: %lu\n", p - bufValue);

	ptnk_datum_t value_data = {(char*)bufValue, p - bufValue};
	DEBUG_OUTF("new value dump %02x %02x %02x %02x %02x\n", bufValue[0], bufValue[1], bufValue[2], bufValue[3], bufValue[4]);
	if(! ptnk_tx_table_put(m_txn->ptnktx, m_ptnktable, datumKeyP, value_data, PTNK_PUT_LEAVE_EXISTING))
	{
		if(ptnk_tx_error(m_txn->ptnktx) == PTNK_EDUPKEY)
		{
			rc = HA_ERR_FOUND_DUPP_KEY;
		}
		else
		{
			DEBUG_OUTF("ptnk_tx_put has failed");
			rc = HA_ERR_INTERNAL_ERROR;	
		}
	}

	::free(bufValue);

	DBUG_RETURN(rc);
}

int
ha_myptnk::update_row(const uchar *old_data, uchar *new_data)
{
	DBUG_ENTER("ha_myptnk::update_row");

	// no active transaction
	if(! m_txn)
	{
		DEBUG_OUTF("error: no active transaction found");
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if(! m_cur)	
	{
		DEBUG_OUTF("update_row called but no active cursor exist");
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
	ptnk_cur_t* cur = static_cast<ptnk_cur_t*>(m_cur);

	// pack key
	DEBUG_OUTF("num keys: %d, key_parts: %d\n", table->s->keys, table->s->key_parts);
	DEBUG_OUTF("primary key idx: %d\n", table->s->primary_key);

	// -- primary key
	KEY* keyP = &table->s->key_info[table->s->primary_key];
	
	char bufKeyP[256];
	size_t sizeKeyP = pack_key_from_mysqlrow(keyP, new_data, bufKeyP);

	ptnk_datum_t datumKeyP = {bufKeyP, sizeKeyP};

	size_t bufsize = calc_packed_rowsize(new_data);
	uchar* bufValue = reinterpret_cast<uchar*>(::malloc(bufsize));
	uchar* p = bufValue;
	DEBUG_OUTF("update_row alloced buf %lu\n", bufsize);

	// copy bitmap for null fields (null_bytes)
	::memcpy(p, new_data, table->s->null_bytes);
	p += table->s->null_bytes;

	// pack each field
	const int nfields = table->s->fields;
	for(int i = 0; i < nfields; ++ i)
	{
		Field* field = table->field[i];	
		ptrdiff_t row_offset = new_data - table->record[0];
		field->move_field_offset(row_offset);
		
		p = field->pack(p, new_data + field->offset(new_data));
		
		field->move_field_offset(-row_offset);
	}
	DEBUG_OUTF("real row size: %lu\n", p - bufValue);

	int rc = 0;

	{
		ptnk_datum_t k, v;
		if(! ptnk_cur_get(&k, &v, cur))
		{
			DEBUG_OUTF("failed to fetch current row from cur\n");	
		}

		if(0 != memcmp(k.dptr, bufKeyP, k.dsize))
		{
			DEBUG_OUTF("update_row error: key of stored row and update_row target row are different!\n");	
		}
	}

	ptnk_datum_t value_data = {(char*)bufValue, p - bufValue};
	DEBUG_OUTF("new value dump %02x %02x %02x %02x %02x\n", bufValue[0], bufValue[1], bufValue[2], bufValue[3], bufValue[4]);
	if(! ptnk_cur_put(cur, value_data))
	{
		DEBUG_OUTF("ptnk_cur_put has failed\n");
		rc = HA_ERR_INTERNAL_ERROR;	
	}

	{
		ptnk_datum_t k, v;
		if(! ptnk_cur_get(&k, &v, cur))
		{
			DEBUG_OUTF("failed to fetch current row from cur\n");	
		}

		if(0 != memcmp(v.dptr, bufValue, v.dsize))
		{
			DEBUG_OUTF("update_row error: val update not applied???\n");
		}
		DEBUG_OUTF("dump %02x %02x %02x %02x %02x\n", v.dptr[0], v.dptr[1], v.dptr[2], v.dptr[3], v.dptr[4]);
	}

	::free(bufValue);

	DBUG_RETURN(rc);
}

int
ha_myptnk::delete_row(const uchar *buf)
{
	DBUG_ENTER("ha_myptnk::delete_row");
		
	// no active transaction
	if(! m_txn)
	{
		DEBUG_OUTF("error: no active transaction found");
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if(! m_cur)	
	{
		DEBUG_OUTF("delete_row called but no active cursor exist");
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
	ptnk_cur_t* cur = static_cast<ptnk_cur_t*>(m_cur);

	if(ptnk_cur_delete(cur))
	{
		// ptnk_cur_delete increments cursor, so skip ptnk_cur_next on next invocation of rnd_next
		m_bSkipCurNext = true;	
	}
	else
	{
		// reached end of records
		rnd_end();	
	}

	DBUG_RETURN(0);
}

bool
ha_myptnk::pack_key_from_mysqlkey(KEY* key, char* dest, size_t* szDest, const uchar* keyd, key_part_map keypart_map)
{
	bool bPackedAll = true;

	DEBUG_OUTF("pack_key_from_mysqlkey\n");
	DEBUG_OUTF("keypart_map: %x\n", keypart_map);

	char* p = dest;

	const int nkey_parts = key->key_parts;
	DEBUG_OUTF("key name: %s key_parts: %d\n", key->name, key->key_parts);
	int i;
	for(i = 0; i < nkey_parts && keypart_map; ++ i, keypart_map >>= 1)
	{
		KEY_PART_INFO* key_part = &key->key_part[i];

		// valid key part data stored in keyd

		DEBUG_OUTF("key_part->null_bit: %d\n", key_part->null_bit);
		if(key_part->null_bit)
		{
			DEBUG_OUTF("null_bit byte: %d\n", *keyd);
			if(*keyd++ == 0)
			{
				*p++ = 0;
			}
			else
			{
				// key null
				*p++ = 1;
				keyd += key_part->store_length;
				continue;
			}
		}

		switch(key_part->field->type())
		{
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_TINY:
			*(unsigned char*)p = *keyd++; p++;
			break;

		case MYSQL_TYPE_SHORT:
			*(unsigned short*)p = *(unsigned short*)keyd; p += 2; keyd += 2;
			break;

		case MYSQL_TYPE_LONG:
			*(unsigned long*)p = htonl(*(unsigned long*)keyd); p += 4; keyd += 4;
			break;

		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_VARCHAR:
			{
				unsigned short len = *(unsigned short*)keyd; keyd += 2;
				*(unsigned short*)p = len; p += 2;
				::memcpy(p, keyd, len);
				p += len; keyd += len;
			}
			break;

		default:
			DEBUG_OUTF("unknown key type: %d, storelen: %d\n", key_part->field->type(), key_part->store_length);
			keyd += key_part->store_length;
			break;
		}
	}
	if(i != nkey_parts) bPackedAll = false;
	for(; i < nkey_parts; ++ i)
	{
		// key_part data does not exist in keyd

		KEY_PART_INFO* key_part = &key->key_part[i];

		if(key_part->null_bit)
		{
			*p++ = 0;
		}

		switch(key_part->field->type())
		{
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_TINY:
			*(unsigned char*)p = 0; p++;
			break;

		case MYSQL_TYPE_SHORT:
			*(unsigned short*)p = 0; p += 2;
			break;

		case MYSQL_TYPE_LONG:
			*(unsigned long*)p = 0; p += 4;
			break;

		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_VARCHAR:
			DEBUG_OUTF("FIXME FIXME\n");
			break;

		default:
			DEBUG_OUTF("unknown key type: %d, storelen: %d\n", key_part->field->type(), key_part->store_length);
			break;
		}
	}

	*szDest = p - dest;
	DEBUG_OUTF("packed key len: %lu\n", *szDest);
	DEBUG_OUTF("packed key dump %02x %02x %02x %02x %02x\n", dest[0], dest[1], dest[2], dest[3], dest[4]);

	return bPackedAll;
}

int
ha_myptnk::index_init(uint idx, bool sorted)
{
	// clear cursor
	if(m_cur) rnd_end();

	active_index = idx;
	return 0;
}

int
ha_myptnk::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag)
{
	int rc;
	DBUG_ENTER("ha_myptnk::index_read_map");

	if(m_cur) rnd_end();
	DEBUG_OUTF("active_index: %d\n", active_index);

	// pack active key
	KEY* ki = &table->s->key_info[active_index];

	char bufKey[256]; size_t sizeKey;
	bool bPackedAll = pack_key_from_mysqlkey(ki, bufKey, &sizeKey, key, keypart_map);

	ptnk_datum_t datumKey = {bufKey, sizeKey};

	int qt = PTNK_MATCH_OR_NEXT;
	switch(find_flag)
	{
	case HA_READ_KEY_EXACT:
		if(bPackedAll)
		{
			qt = PTNK_MATCH_EXACT;
		}
		else
		{
			// not all key_parts have been specified	
			// packed key for non-specified key_parts are zero-filled,
			// so query type must be loosened to match them.
			qt = PTNK_MATCH_OR_NEXT;
		}
		break;

	case HA_READ_KEY_OR_NEXT:
		qt = PTNK_MATCH_OR_NEXT;
		break;

	case HA_READ_KEY_OR_PREV:
		qt = PTNK_MATCH_OR_PREV;
		break;

	case HA_READ_AFTER_KEY:
		qt = PTNK_AFTER;
		break;

	case HA_READ_BEFORE_KEY:
		qt = PTNK_BEFORE;
		break;

	default:
		DEBUG_OUTF("unsupported find_flag: %d\n", find_flag);
		break;
	};

	if(active_index == table->s->primary_key)
	{
		// primary key
		m_cur = static_cast<void*>(::ptnk_query(m_txn->ptnktx, m_ptnktable, datumKey, qt));
		m_bSecondaryKeyActive = false;
	}
	else
	{
		// secondary key
		m_cur = static_cast<void*>(::ptnk_query(m_txn->ptnktx, m_ptnktable_sidx[active_index-1], datumKey, qt));
		m_bSecondaryKeyActive = true;
	}

	DEBUG_OUTF("query result: %p\n", m_cur);

	m_bSkipCurNext = true;

	// read first record using idx
	DBUG_RETURN(rnd_next(buf));
}

int
ha_myptnk::index_next(uchar *buf)
{
	int rc;
	DBUG_ENTER("ha_myptnk::index_next");

	// if(active_index == primary key)
	rc = rnd_next(buf);

	DBUG_RETURN(rc);
}

int
ha_myptnk::index_prev(uchar *buf)
{
	int rc;
	DBUG_ENTER("ha_myptnk::index_prev");
	WARN_STUB;
	MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
	rc= HA_ERR_WRONG_COMMAND;
	MYSQL_INDEX_READ_ROW_DONE(rc);
	DBUG_RETURN(rc);
}

int
ha_myptnk::index_first(uchar *buf)
{
	int rc;
	DBUG_ENTER("ha_myptnk::index_first");

	if(active_index == table->s->primary_key)
	{
		rnd_init(false);
		rc = rnd_next(buf);
	}
	else
	{
		// no active transaction
		if(! m_txn)
		{
			DEBUG_OUTF("error: no active transaction found\n");
			DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
		}

		m_cur = static_cast<void*>(::ptnk_cur_front(m_txn->ptnktx, m_ptnktable_sidx[active_index-1]));
		m_bSkipCurNext = true;
		m_bSecondaryKeyActive = true;

		rc = rnd_next(buf);
	}

	DBUG_RETURN(rc);
}

int
ha_myptnk::index_last(uchar *buf)
{
	int rc;
	DBUG_ENTER("ha_myptnk::index_last");
	WARN_STUB;
	MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
	rc= HA_ERR_WRONG_COMMAND;
	MYSQL_INDEX_READ_ROW_DONE(rc);
	DBUG_RETURN(rc);
}

int
ha_myptnk::rnd_init(bool scan)
{
	DBUG_ENTER("ha_myptnk::rnd_init");

	// no active transaction
	if(! m_txn)
	{
		DEBUG_OUTF("error: no active transaction found\n");
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	m_cur = static_cast<void*>(::ptnk_cur_front(m_txn->ptnktx, m_ptnktable));
	m_bSkipCurNext = true;
	m_bSecondaryKeyActive = false;

	DBUG_RETURN(0);
}

int
ha_myptnk::rnd_end()
{
	DBUG_ENTER("ha_myptnk::rnd_end");
	if(m_cur)
	{
		ptnk_cur_t* cur = static_cast<ptnk_cur_t*>(m_cur);
		::ptnk_cur_close(cur);
		m_cur = NULL;
	}
	DBUG_RETURN(0);
}

int
ha_myptnk::rnd_next(uchar *buf)
{
	int rc;
	DBUG_ENTER("ha_myptnk::rnd_next");

	// no active transaction
	if(! m_txn)
	{
		DEBUG_OUTF("error: no active transaction found");
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
	if(! m_cur)
	{
		// no record found
		table->status = STATUS_NOT_FOUND;
		rc = HA_ERR_END_OF_FILE;
		MYSQL_READ_ROW_DONE(rc);
		DBUG_RETURN(rc);
	}
	ptnk_cur_t* cur = static_cast<ptnk_cur_t*>(m_cur);

	if(m_bSkipCurNext)
	{
		m_bSkipCurNext = false;

		// no increment cursor on first record read
	}
	else
	{
		// increment cursor
		if(! ::ptnk_cur_next(cur))
		{
			// no record found
			table->status = STATUS_NOT_FOUND;
			rc = HA_ERR_END_OF_FILE;
			MYSQL_READ_ROW_DONE(rc);
			DBUG_RETURN(rc);
		}
	}

	// get current cursor value
	ptnk_datum_t key, value;
	if(! m_bSecondaryKeyActive)
	{
		// primary key scan
		if(! ::ptnk_cur_get(&key, &value, cur))
		{
			rc = HA_ERR_INTERNAL_ERROR;
			MYSQL_READ_ROW_DONE(rc);
			DBUG_RETURN(rc);
		}
	}
	else
	{
		// secondary key scan
		// -- get primary key from secondary index
		ptnk_datum_t pkey;
		if(! ::ptnk_cur_get(&key, &pkey, cur))
		{
			rc = HA_ERR_INTERNAL_ERROR;
			MYSQL_READ_ROW_DONE(rc);
			DBUG_RETURN(rc);
		}

		// -- query main table
		value = ::ptnk_tx_table_get(m_txn->ptnktx, m_ptnktable, pkey);
		if(value.dsize < 0)
		{
			rc = HA_ERR_INTERNAL_ERROR;
			MYSQL_READ_ROW_DONE(rc);
			DBUG_RETURN(rc);
		}
	}

	{
		const char* p = value.dptr;

		// copy bitmap for null fields (null_bytes)
		::memcpy(buf, p, table->s->null_bytes);
		p += table->s->null_bytes;

		// unpack each field
		const int nfields = table->s->fields;
		for(int i = 0; i < nfields; ++ i)
		{
			Field* field = table->field[i];
			p = (char*)field->unpack(buf + field->offset(buf), (uchar*)p);
		}
		DEBUG_OUTF("unpacked buffer: %lu\n", p - value.dptr);
		DEBUG_OUTF("value size: %lu\n", value.dsize);

		rc = 0;
	}

	MYSQL_READ_ROW_DONE(rc);
	DBUG_RETURN(rc);
}

void
ha_myptnk::position(const uchar *record)
{
	DBUG_ENTER("ha_myptnk::position");
	WARN_STUB;
	DBUG_VOID_RETURN;
}

int
ha_myptnk::rnd_pos(uchar *buf, uchar *pos)
{
	int rc;
	DBUG_ENTER("ha_myptnk::rnd_pos");
	WARN_STUB;
	MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
	rc= HA_ERR_WRONG_COMMAND;
	MYSQL_READ_ROW_DONE(rc);
	DBUG_RETURN(rc);
}

int
ha_myptnk::info(uint flag)
{
	DBUG_ENTER("ha_myptnk::info");
	WARN_STUB;
	DBUG_RETURN(0);
}

#define EXTRA_STUB(op) \
	case op: DEBUG_OUTF("extra stub: " #op "\n"); break;

int
ha_myptnk::extra(enum ha_extra_function operation)
{
	DBUG_ENTER("ha_myptnk::extra");
	// note: see include/my_base.h for operation defs

	switch(operation)
	{
	EXTRA_STUB(HA_EXTRA_NORMAL);
	EXTRA_STUB(HA_EXTRA_QUICK);
	EXTRA_STUB(HA_EXTRA_NOT_USED);
	EXTRA_STUB(HA_EXTRA_CACHE);
	EXTRA_STUB(HA_EXTRA_NO_CACHE);
	EXTRA_STUB(HA_EXTRA_NO_READCHECK);
	EXTRA_STUB(HA_EXTRA_READCHECK);
	EXTRA_STUB(HA_EXTRA_KEYREAD);
	EXTRA_STUB(HA_EXTRA_NO_KEYREAD);
	EXTRA_STUB(HA_EXTRA_NO_USER_CHANGE);
	EXTRA_STUB(HA_EXTRA_KEY_CACHE);
	EXTRA_STUB(HA_EXTRA_NO_KEY_CACHE);
	EXTRA_STUB(HA_EXTRA_WAIT_LOCK);
	EXTRA_STUB(HA_EXTRA_NO_WAIT_LOCK);
	EXTRA_STUB(HA_EXTRA_WRITE_CACHE);
	EXTRA_STUB(HA_EXTRA_FLUSH_CACHE);
	EXTRA_STUB(HA_EXTRA_NO_KEYS);
	EXTRA_STUB(HA_EXTRA_KEYREAD_CHANGE_POS);
	EXTRA_STUB(HA_EXTRA_REMEMBER_POS);
	EXTRA_STUB(HA_EXTRA_RESTORE_POS);
	EXTRA_STUB(HA_EXTRA_REINIT_CACHE);
	EXTRA_STUB(HA_EXTRA_FORCE_REOPEN);
	EXTRA_STUB(HA_EXTRA_FLUSH);
	EXTRA_STUB(HA_EXTRA_NO_ROWS);
	EXTRA_STUB(HA_EXTRA_RESET_STATE);
	EXTRA_STUB(HA_EXTRA_IGNORE_DUP_KEY);
	EXTRA_STUB(HA_EXTRA_NO_IGNORE_DUP_KEY);
	EXTRA_STUB(HA_EXTRA_PREPARE_FOR_DROP);
	EXTRA_STUB(HA_EXTRA_PREPARE_FOR_UPDATE);
	EXTRA_STUB(HA_EXTRA_PRELOAD_BUFFER_SIZE);
	EXTRA_STUB(HA_EXTRA_CHANGE_KEY_TO_UNIQUE);
	EXTRA_STUB(HA_EXTRA_CHANGE_KEY_TO_DUP);
	EXTRA_STUB(HA_EXTRA_KEYREAD_PRESERVE_FIELDS);
	EXTRA_STUB(HA_EXTRA_MMAP);
	EXTRA_STUB(HA_EXTRA_IGNORE_NO_KEY);
	EXTRA_STUB(HA_EXTRA_NO_IGNORE_NO_KEY);
	EXTRA_STUB(HA_EXTRA_MARK_AS_LOG_TABLE);
	EXTRA_STUB(HA_EXTRA_WRITE_CAN_REPLACE);
	EXTRA_STUB(HA_EXTRA_WRITE_CANNOT_REPLACE);
	EXTRA_STUB(HA_EXTRA_DELETE_CANNOT_BATCH);
	EXTRA_STUB(HA_EXTRA_UPDATE_CANNOT_BATCH);
	EXTRA_STUB(HA_EXTRA_INSERT_WITH_UPDATE);
	EXTRA_STUB(HA_EXTRA_PREPARE_FOR_RENAME);
	};
	DBUG_RETURN(0);
}

int
ha_myptnk::delete_all_rows()
{
	DBUG_ENTER("ha_myptnk::delete_all_rows");
	WARN_STUB;
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int
ha_myptnk::truncate()
{
	DBUG_ENTER("ha_myptnk::truncate");
	WARN_STUB;
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int
ha_myptnk::start_stmt(THD *thd, thr_lock_type lock_type)
{
	DBUG_ENTER("ha_myptnk::start_stmt");

	switch(lock_type)
	{
	case F_RDLCK:
		DEBUG_OUTF("start_stmt F_RDLCK\n");
		break;

	case F_WRLCK:
		DEBUG_OUTF("start_stmt F_WRLCK\n");
		break;

	case F_UNLCK:
		DEBUG_OUTF("start_stmt F_UNLCK\n");
		break;
	}
	
	DBUG_RETURN(0);
}

int
ha_myptnk::external_lock(THD *thd, int lock_type)
{
	DBUG_ENTER("ha_myptnk::external_lock");

	int errcode = 0;

	switch(lock_type)
	{
	case F_RDLCK:
		DEBUG_OUTF("external_lock F_RDLCK\n");
		break;

	case F_WRLCK:
		DEBUG_OUTF("external_lock F_WRLCK\n");
		break;

	case F_UNLCK:
		DEBUG_OUTF("external_lock F_UNLCK\n");
		break;
	}

	myptnk_txn* txn = static_cast<myptnk_txn*>(thd_get_ha_data(thd, ht));

	if(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)) DEBUG_OUTF("non autocommit mode\n");
	if(thd_test_options(thd, OPTION_BEGIN)) DEBUG_OUTF("thd begin txn\n");

	bool acmode = ! thd_test_options(thd, OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT);

	if(lock_type != F_UNLCK)
	{
		if(! txn)
		{
			txn = new myptnk_txn(m_table_share->ptnkdb, lock_type == F_RDLCK);
			thd_set_ha_data(thd, ht, txn);
			trans_register_ha(thd, acmode ? FALSE : TRUE, ht);
		}
		else
		{
			txn->incCount();
			if(txn->readonly && (lock_type == F_WRLCK))
			{
				txn->readonly = false;	
			}
		}
	}
	else
	{
		if(! txn)
		{
			DEBUG_OUTF("external_lock F_UNLCK called but transaction not found");
		}
		else
		{
			if(acmode && txn->decCount())
			{
				errcode = myptnk_commit(ht, thd, true);
				txn = NULL;
			}
		}
	}

	m_txn = txn;
	DBUG_RETURN(errcode);
}

#define STORE_LOCK_STUB(op) \
	case op: DEBUG_OUTF("lock stub: " #op "\n"); break;

THR_LOCK_DATA**
ha_myptnk::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
	if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) lock.type=lock_type;
	*to++= &lock;
	
	switch(lock_type)
	{
		STORE_LOCK_STUB(TL_UNLOCK)
		STORE_LOCK_STUB(TL_READ_DEFAULT)
		STORE_LOCK_STUB(TL_READ)
		STORE_LOCK_STUB(TL_READ_WITH_SHARED_LOCKS)
		STORE_LOCK_STUB(TL_READ_HIGH_PRIORITY)
		STORE_LOCK_STUB(TL_READ_NO_INSERT)
		STORE_LOCK_STUB(TL_WRITE_ALLOW_WRITE)
		STORE_LOCK_STUB(TL_WRITE_CONCURRENT_INSERT)
		STORE_LOCK_STUB(TL_WRITE_DELAYED)
		STORE_LOCK_STUB(TL_WRITE_DEFAULT)
		STORE_LOCK_STUB(TL_WRITE_LOW_PRIORITY)
		STORE_LOCK_STUB(TL_WRITE)
		STORE_LOCK_STUB(TL_WRITE_ONLY)
	}

	return to;
}

int
ha_myptnk::delete_table(const char *name)
{
	DBUG_ENTER("ha_myptnk::delete_table");
	int rc = 0;

	if (!(m_table_share = get_share(name, table))) DBUG_RETURN(1);

	copy_table_name(name);
	m_table_share->open(O_RDWR);

	ptnk_db_t* db = m_table_share->ptnkdb;
	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	// drop table for secondary keys
	for(int i = 1 ;; ++ i)
	{
		char idx_table_name[1024];
		sprintf(idx_table_name, "%s/%d", m_table_name, i);
		if(! ::ptnk_tx_table_drop_cstr(tx, idx_table_name))
		{
			break;
		}
	}

	if(! ::ptnk_tx_table_drop_cstr(tx, m_table_name))
	{
		DEBUG_OUTF("failed to drop table: %s\n", m_table_name);
		rc = HA_ERR_INTERNAL_ERROR;
	}

	// drop db if table other than the default table (idx 0) doesn't exist
	bool bDropDB = (! ::ptnk_tx_table_get_name_cstr(tx, 1));

	if(! ::ptnk_tx_end(tx, PTNK_TX_COMMIT))
	{
		DEBUG_OUTF("failed to commit delete table txn\n");
		rc = HA_ERR_LOCK_DEADLOCK;	
	}

	if(bDropDB)
	{
		DEBUG_OUTF("no tables left -> deleting db file\n");
		rc = m_table_share->dropdb();	
	}

	free_share(m_table_share);
	m_table_share = NULL;

	DBUG_RETURN(rc);
}

int
ha_myptnk::rename_table(const char * from, const char * to)
{
	DBUG_ENTER("ha_myptnk::rename_table ");
	DEBUG_OUTF("rename_table name: %s -> %s\n", from, to);
	WARN_STUB;
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

ha_rows
ha_myptnk::records_in_range(uint inx, key_range *min_key, key_range *max_key)
{
	DBUG_ENTER("ha_myptnk::records_in_range");
	WARN_STUB;
	DBUG_RETURN(10);
}

int
ha_myptnk::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	DBUG_ENTER("ha_myptnk::create");

	if(m_table_share)
	{
		DEBUG_OUTF("table share already exists? handler reused?\n");
	}
	else
	{
		if (!(m_table_share = get_share(name, table))) DBUG_RETURN(1);
	}

	copy_table_name(name);
	m_table_share->open(O_CREAT | O_RDWR);

	ptnk_db_t* db = m_table_share->ptnkdb;
	ptnk_tx_t* tx = ::ptnk_tx_begin(db);

	if(! ::ptnk_tx_table_create_cstr(tx, m_table_name))
	{
		DEBUG_OUTF("failed to create table: %s\n", m_table_name);
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	// create table for secondary keys
	for(int i = 1; i < table_arg->s->keys; ++ i)
	{
		char idx_table_name[1024];
		sprintf(idx_table_name, "%s/%d", m_table_name, i);
		if(! ::ptnk_tx_table_create_cstr(tx, idx_table_name))
		{
			DEBUG_OUTF("failed to create table (secondary idx): %s\n", idx_table_name);
			DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
		}
	}

	if(! ::ptnk_tx_end(tx, PTNK_TX_COMMIT))
	{
		DEBUG_OUTF("failed to commit create table txn\n");
		DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
	}

	DBUG_RETURN(0);
}

struct st_mysql_storage_engine myptnk_storage_engine= { MYSQL_HANDLERTON_INTERFACE_VERSION };
static struct st_mysql_sys_var* myptnk_system_variables[]= { NULL };
static struct st_mysql_show_var func_status[]= { {0,0,SHOW_UNDEF} };

mysql_declare_plugin(myptnk)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&myptnk_storage_engine,
	"MYPTNK",
	"nyaxt",
	"ptnk log-structured database for MySQL",
	PLUGIN_LICENSE_BSD,
	myptnk_init_func,                            /* Plugin Init */
	myptnk_done_func,                            /* Plugin Deinit */
	0x0001 /* 0.1 */,
	func_status,                                 /* status variables */
	myptnk_system_variables,                     /* system variables */
	NULL                                         /* config options */
}
mysql_declare_plugin_end;
