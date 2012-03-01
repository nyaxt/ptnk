#ifndef _ptnk_partitionedpageio_h_
#define _ptnk_partitionedpageio_h_

#include <array>

#include <thread>

#include "pageio.h"
#include "pageiomem.h"

namespace ptnk
{

class Helper;

class PartitionedPageIO : public PageIO
{
public:
	//! C-tor
	/*!
	 *	@param dbprefix
	 *		ex. "/db/file/path/dbname"
	 */
	PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode = 0644);
	void attachHelper(Helper* helper);

	~PartitionedPageIO();

	typedef pair<std::string, part_id_t> partfile_t;
	typedef std::vector<partfile_t> Vpartfile_t;

	void dump(std::ostream& s) const;

	typedef std::function<void ()> hook_t;
	//! add hook func. to be called when adding new partition
	void setHookAddNewPartition(const hook_t& h)
	{
		m_hook_addNewPartition = h;	
	}

	//! scan for partitioned db files
	static void scanFiles(Vpartfile_t* files, const char* dbprefix);

	//! delete all db files for _dbprefix_
	static void drop(const char* dbprefix);

	// ====== implements PageIO interface ======

	virtual pair<Page, page_id_t> newPage();

	virtual Page readPage(page_id_t pgid);
	virtual void sync(page_id_t pgid);
	virtual void syncRange(page_id_t pgidStart, page_id_t pgidEnd);

	virtual page_id_t getLastPgId() const;
	virtual local_pgid_t getPartLastLocalPgId(part_id_t ptid) const;

	virtual bool needInit() const;

	page_id_t alignCompactionThreshold(page_id_t threshold) const;

	virtual void newPart(bool bForce = false);
	virtual void discardOldPages(page_id_t threshold);

	virtual void dumpStat() const;

	// ====== inspection funcs for test ======
	
	size_t numPartitions_() const;

private:
	//! open partitioned db files and populate m_parts
	/*!
	 *	@return
	 *		true if usable existing db file was found
	 */
	bool openFiles();

	//! add new partition
	/*!
	 *	@caution m_mtxAlloc must be locked when calling this method.
	 */
	void addNewPartition_unsafe();

	void scanLastPgId();

	//! alloc more pages
	void expandTo(page_id_t pgid);

	//! db prefix str. see C-tor param
	std::string m_dbprefix;

	//! db file creat(2) perm
	int m_mode;	

	//! opts passed to partition handler PageIOs
	ptnk_opts_t m_opts;

	bool m_needInit;

	std::array<unique_ptr<MappedFile>, PTNK_PARTID_MAX+2> m_parts;

	std::mutex m_mtxAlloc;

	//! last alloc-ed pgid
	volatile page_id_t m_pgidLast;

	//! first loaded partition id
	part_id_t m_partidFirst;

	//! last partition id
	part_id_t m_partidLast;

	//! helper thr
	Helper* m_helper;

	//! make sure that helper job do not duplicate
	/*!
	 *	@sa newPage() impl.
	 */
	bool m_isHelperInvoked;

	//! hook func to be called when adding new partition
	hook_t m_hook_addNewPartition;
};
inline
std::ostream& operator<<(std::ostream& s, const PartitionedPageIO& o)
{ o.dump(s); return s; }

} // end of namespace ptnk

#endif // _ptnk_partitionedpageio_h_
