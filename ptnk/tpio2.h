#ifndef _ptnk_tpio2_h_
#define _ptnk_tpio2_h_

#include "pageio.h"
#include "stm.h"
#include "pol.h"

// Transactional PageIO impl. using stm.h

namespace ptnk
{

struct TPIOStat
{
	unsigned int nUniquePages;

	unsigned int nRead;
	unsigned int nReadOvr;
	unsigned int nReadOvrLocal;

	unsigned int nModifyPage;
	unsigned int nNewOvr;

	unsigned int nSync;

	unsigned int nNotifyOldLink;

	TPIOStat() :
		nRead(0),
		nReadOvr(0),
		nReadOvrLocal(0),
		nModifyPage(0),
		nNewOvr(0),
		nSync(0),
		nNotifyOldLink(0)
	{ /* NOP */ }
};

class TPIO2;

class TPIO2TxSession : public PageIO
{
public:
	~TPIO2TxSession();

	void dump(std::ostream& s) const;

	bool tryCommit();

	const TPIOStat& stat() const
	{
		return m_stat;	
	}

	// ====== implements PageIO interface ======
	pair<Page, page_id_t> newPage();

	Page readPage(page_id_t page); 
	Page modifyPage(const Page& page, mod_info_t* mod);
	void discardPage(page_id_t pgid, mod_info_t* mod);

	void sync(page_id_t pgid);

	page_id_t getLastPgId() const;

	void notifyPageWOldLink(page_id_t pgid);
	page_id_t updateLink(page_id_t pgidOld);

	// ====== start page accessor ======

	page_id_t pgidStartPage()
	{
		return m_lovr->pgidStartPage();	
	}

	void setPgidStartPage(page_id_t pgid)
	{
		m_lovr->setPgidStartPage(pgid);
	}

protected:
	friend class TPIO2; // give access to c-tor
	TPIO2TxSession(TPIO2* tpio, unique_ptr<LocalOvr>&& lovr);

	PageIO* backend() const;

	PagesOldLink* oldlink()
	{
		return &reinterpret_cast<OvrExtra*>(m_lovr->getExtra())->oldlink;
	};

	void addOvr(page_id_t pgidOrig, page_id_t pgidOvr)
	{
		m_lovr->addOvr(pgidOrig, pgidOvr);	
	}

	void loadStreak(BufferCRef bufStreak);

private:
	struct OvrExtra : public LocalOvr::ExtraData
	{
		~OvrExtra();

		PagesOldLink oldlink;
	};

	TPIO2* m_tpio;
	unique_ptr<LocalOvr> m_lovr;
	Vpage_id_t m_pagesModified;
	TPIOStat m_stat;
};
inline
std::ostream& operator<<(std::ostream& s, const TPIO2TxSession& o)
{ o.dump(s); return s; }

class TPIO2
{
public:
	TPIO2(boost::shared_ptr<PageIO> backend);

	~TPIO2();

	void dump(std::ostream& s) const;

	unique_ptr<TPIO2TxSession> newTransaction();
	bool tryCommit(TPIO2TxSession* tx, ver_t verW = TXID_INVALID);

	void rebase(bool force);
	void refreshOldPages(page_id_t threshold);

	PageIO* backend()
	{
		return m_backend.get();	
	}

	const TPIOStat& stat()
	{
		return m_stat;	
	}

private:
	void syncDelayed(const Vpage_id_t& pagesModified);

	void restoreState();

	boost::shared_ptr<PageIO> m_backend;

	shared_ptr<ActiveOvr> m_aovr;

	TPIOStat m_stat;
};
inline
std::ostream& operator<<(std::ostream& s, const TPIO2& o)
{ o.dump(s); return s; }

inline
bool
TPIO2TxSession::tryCommit()
{
	return m_tpio->tryCommit(this);	
}

inline
PageIO*
TPIO2TxSession::backend() const
{
	return m_tpio->backend();
}

} // end of namespace ptnk

#endif // _ptnk_tpio2_h_
