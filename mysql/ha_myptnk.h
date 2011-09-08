#ifndef _ha_myptnk_h_
#define _ha_myptnk_h_

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "my_global.h"
#include "thr_lock.h"
#include "handler.h"
#include "my_base.h"

#undef __cplusplus
#define PTNK_ADD_PREFIX
// force load only C API as ptnk native C++ plugin assumes RTTI/exceptions
extern "C" {
#include <ptnk.h>
}
#define __cplusplus

struct myptnk_share;
struct myptnk_txn;

class ha_myptnk : public handler
{
public:
	ha_myptnk(handlerton *hton, TABLE_SHARE *table_arg);
	~ha_myptnk() { /* NOP */ }

	const char *table_type() const { return "myptnk"; }
	const char *index_type(uint inx) { return "BTREE"; }

	const char **bas_ext() const;

	ulonglong table_flags() const { return HA_NULL_IN_KEY | HA_REC_NOT_IN_SEQ | HA_BINLOG_STMT_CAPABLE; } // HA_PRIMARY_KEY_IN_READ_INDEX | HA_PRIMARY_KEY_REQUIRED_FOR_POSITION
	ulong index_flags(uint inx, uint part, bool all_parts) const { return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER; }

	uint max_supported_record_length() const { return 2048; }
	uint max_supported_keys()          const { return 8; }
	uint max_supported_key_parts()     const { return 8; }
	uint max_supported_key_length()    const { return 255; }

	virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }
	virtual double read_time(uint, uint, ha_rows rows) { return (double) rows /  20.0+1; }

	int open(const char *name, int mode, uint test_if_locked);
	int close(void);
	int write_row(uchar *buf);
	int update_row(const uchar *old_data, uchar *new_data);
	int delete_row(const uchar *buf);
	int index_init(uint idx, bool sorted);
	int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag);
	int index_next(uchar *buf);
	int index_prev(uchar *buf);
	int index_first(uchar *buf);
	int index_last(uchar *buf);
	int rnd_init(bool scan);
	int rnd_end();
	int rnd_next(uchar *buf);
	int rnd_pos(uchar *buf, uchar *pos);
	void position(const uchar *record);
	int info(uint);
	int extra(enum ha_extra_function operation);
	int start_stmt(THD *thd, thr_lock_type lock_type);
	int external_lock(THD *thd, int lock_type);
	int delete_all_rows(void);
	int truncate();
	ha_rows records_in_range(uint inx, key_range *min_key,
			key_range *max_key);
	int delete_table(const char *from);
	int rename_table(const char * from, const char * to);
	int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);

	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);

private:
	size_t calc_packed_rowsize(uchar* buf);
	void copy_table_name(const char* name);

	size_t pack_key_from_mysqlrow(KEY* key, uchar *buf, char* dest);
	bool pack_key_from_mysqlkey(KEY* key, char* dest, size_t* szDest, const uchar* keyd, key_part_map keypart_map, int filler);

	THR_LOCK_DATA lock;
	myptnk_share* m_table_share;
	myptnk_txn* m_txn;

	char* m_table_name;
	void* m_ptnktable;
	void* m_ptnktable_sidx[8];
	bool m_bSecondaryKeyActive;
	void* m_cur;
	bool m_bSkipCurNext;
};

#endif // _ha_myptnk_h_
