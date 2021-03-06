#ifndef _ptnk_db_h_
#define _ptnk_db_h_

#include "common.h"
#include "query.h"
#include "toc.h"

namespace ptnk
{

class TPIO;
class TPIOTxSession;

class PageIO;
class Helper;

class DB
{
public:
	DB(const char* filename = NULL, ptnk_opts_t opts = ODEFAULT, int mode = 0644);
	DB(const shared_ptr<PageIO>& pio, ptnk_opts_t opts = ODEFAULT);

	void initCommon();

	~DB();

	static void drop(const char* filename);

	ssize_t get(BufferCRef key, BufferRef value);
	void get(BufferCRef key, Buffer* value)
	{
		value->setValsize(get(key, value->wref()));
	}
	void put(BufferCRef key, BufferCRef value, put_mode_t mode = PUT_UPDATE);

	ssize_t get_k32u(uint32_t nkey, BufferRef value)
	{
		uint32_t kb = PTNK_BSWAP32(nkey); BufferCRef key(&kb, 4);
		return get(key, value);
	}
	void get_k32u(uint32_t nkey, Buffer* value)
	{
		uint32_t kb = PTNK_BSWAP32(nkey); BufferCRef key(&kb, 4);
		get(key, value);
	}
	void put_k32u(uint32_t nkey, BufferCRef value)
	{
		uint32_t kb = PTNK_BSWAP32(nkey); BufferCRef key(&kb, 4);
		put(key, value);
	}

	//! transaction class
	class Tx
	{
	public:
		~Tx();

		void tableCreate(BufferCRef table);
		void tableDrop(BufferCRef table);
		ssize_t tableGetName(int idx, BufferRef name);
		void tableGetName(int idx, Buffer* name)
		{
			name->setValsize(tableGetName(idx, name->wref()));	
		}

		ssize_t get(BufferCRef table, BufferCRef key, BufferRef value);
		void get(BufferCRef table, BufferCRef key, Buffer* value)
		{
			value->setValsize(get(table, key, value->wref()));
		}
		void put(BufferCRef table, BufferCRef key, BufferCRef value, put_mode_t mode = PUT_UPDATE);

		ssize_t get(TableOffCache* table, BufferCRef key, BufferRef value);
		void get(TableOffCache* table, BufferCRef key, Buffer* value)
		{
			value->setValsize(get(table, key, value->wref()));
		}
		void put(TableOffCache* table, BufferCRef key, BufferCRef value, put_mode_t mode = PUT_UPDATE);

		ssize_t get(BufferCRef key, BufferRef value);
		void get(BufferCRef key, Buffer* value)
		{
			value->setValsize(get(key, value->wref()));
		}
		void put(BufferCRef key, BufferCRef value, put_mode_t mode = PUT_UPDATE);

		ssize_t get_k32u(uint32_t nkey, BufferRef value)
		{
			uint32_t kb = PTNK_BSWAP32(nkey); BufferCRef key(&kb, 4);
			return get(key, value);
		}
		void get_k32u(uint32_t nkey, Buffer* value)
		{
			uint32_t kb = PTNK_BSWAP32(nkey); BufferCRef key(&kb, 4);
			get(key, value);
		}
		void put_k32u(uint32_t nkey, BufferCRef value)
		{
			uint32_t kb = PTNK_BSWAP32(nkey); BufferCRef key(&kb, 4);
			put(key, value);
		}

		struct cursor_t;
		static void curClose(cursor_t* cur);

		cursor_t* curFront(BufferCRef table);
		cursor_t* curBack(BufferCRef table);
		cursor_t* curQuery(BufferCRef table, const query_t& q);

		cursor_t* curFront(TableOffCache* table);
		cursor_t* curBack(TableOffCache* table);
		cursor_t* curQuery(TableOffCache* table, const query_t& q);
		
		void curGet(BufferRef key, ssize_t* szKey, BufferRef value, ssize_t* szValue, cursor_t* cur);
		void curGet(Buffer* key, Buffer* value, cursor_t* cur)
		{
			curGet(key->wref(), key->pvalsize(), value->wref(), value->pvalsize(), cur);
		}

		void curPut(cursor_t* cur, BufferCRef key);
		bool curDelete(cursor_t* cur);

		bool curNext(cursor_t* cur);
		bool curPrev(cursor_t* cur);

		bool tryCommit();

		void dumpStat() const;

		TPIOTxSession* pio()
		{
			return m_pio.get();	
		}

	private:
		Tx(DB* db, unique_ptr<TPIOTxSession> pio);

		cursor_t* curNew(BufferCRef table);
		cursor_t* curNew(TableOffCache* table);

		bool m_bCommitted;

		DB* m_db;
		unique_ptr<TPIOTxSession> m_pio;

		friend class DB;
	};
	friend class Tx;
	Tx* newTransaction();

	void rebase(bool force = true);
	void newPart(bool doRebase = true);
	void compactFast();

	// ====== inspectors ======
	
	void dump() const;
	void dumpGraph(FILE* fp) const;

	void dumpStat() const;

	void dumpAll();

	TPIO* tpio_()
	{
		return m_tpio.get();
	}

private:
	void handleHookAddNewPartition();

	shared_ptr<PageIO> m_pio;
	unique_ptr<TPIO> m_tpio;

	unique_ptr<Helper> m_helper;
};

} // end of namespace ptnk

#endif // _ptnk_db_h_
