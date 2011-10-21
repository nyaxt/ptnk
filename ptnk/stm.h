#ifndef _ptnk_stm_h_
#define _ptnk_stm_h_

#include <iostream>

#include "bitvector.h"
#include "types.h"

namespace ptnk
{

typedef tx_id_t ver_t;

enum { TPIO_NHASH = 64 };

struct OvrEntry
{
	page_id_t pgidOrig;

	page_id_t pgidOvr;

	ver_t ver;

	OvrEntry* prev;
};
std::ostream& operator<<(std::ostream& s, const OvrEntry& e);

enum ovr_status_t 
{
	OVR_NONE, //!< the page got no ovrs
	OVR_GLOBAL, //!< there is global override for the page
	OVR_LOCAL, //!< there is local override for the page
};

inline int pgidhash(page_id_t pgid)
{
	return pgid % TPIO_NHASH;
}

class PgidBloomFilter
{
public:
	PgidBloomFilter()
	{
		::memset(m_bvBloomLocalOvrs, 0, sizeof(m_bvBloomLocalOvrs));	
	}

	void add(page_id_t pgid) {}
	bool mayContain(page_id_t pgid) { return true; }
	bool mayContain(const PgidBloomFilter& o) { return true; }

private:
	char m_bvBloomLocalOvrs[16];
};

class __attribute__ ((aligned (8))) LocalOvr
{
public:
	LocalOvr(OvrEntry* hashOvrs[], ver_t verRead);
	~LocalOvr();

	pair<page_id_t, ovr_status_t> searchOvr(page_id_t pgid);
	void addOvr(page_id_t pgidOrig, page_id_t pgidOvr);

	void dump(std::ostream& s) const;

	class ExtraData
	{
	public:
		virtual ~ExtraData() = 0;	
	};
	void attachExtra(unique_ptr<ExtraData>&& extra);
	ExtraData* getExtra() { return m_extra.get(); }

private:
	enum { TAG_TXVER_LOCAL = 0 };

	Vpage_id_t m_pgidOrigs;
	Vpage_id_t m_pgidOvrs;

	OvrEntry* m_hashOvrs[TPIO_NHASH];

	PgidBloomFilter m_bfOvrs;

	//! tx ver id of read snapshot
	ver_t m_verRead;

	//! tx ver id of this tx
	ver_t m_verWrite;

	friend class ActiveOvr;
	bool checkConflict(LocalOvr* other);
	LocalOvr* m_prev;
	bool m_mergeOngoing;
	bool m_merged;

	unique_ptr<ExtraData> m_extra;
};

inline
std::ostream& operator<<(std::ostream& s, const LocalOvr& o)
{ o.dump(s); return s; }

class ActiveOvr
{
public:
	ActiveOvr();
	~ActiveOvr();

	unique_ptr<LocalOvr> newTx();
	bool tryCommit(unique_ptr<LocalOvr>& lovr);

	void dump(std::ostream& s) const;

private:
	void merge(LocalOvr* lovr);

	OvrEntry* m_hashOvrs[TPIO_NHASH];

	ver_t m_verRebase;

	//! latest verified tx
	/*!
	 *	tx in this linked-list are ensured that they do not conflict each other
	 */
	LocalOvr* m_lovrVerifiedTip;
};

inline
std::ostream& operator<<(std::ostream& s, const ActiveOvr& o)
{ o.dump(s); return s; }

} // end of namespace ptnk

#endif // _ptnk_stm_h_
