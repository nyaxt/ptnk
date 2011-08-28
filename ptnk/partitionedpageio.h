#ifndef _ptnk_partitionedpageio_h_
#define _ptnk_partitionedpageio_h_

#include <boost/ptr_container/ptr_array.hpp>
#include <boost/thread.hpp>

#include "pageio.h"
#include "pageiomem.h"

namespace ptnk
{

class MappedFile
{
public:
	enum
	{
		NUM_PAGES_ALLOC_ONCE = 256,
	};

	static MappedFile* openExisting(part_id_t partid, const char* filename, ptnk_opts_t opts);
	static MappedFile* createNew(part_id_t partid, const char* filename, ptnk_opts_t opts, int mode);
	~MappedFile();

	page_id_t alloc();
	char* calcPtr(local_pgid_t pgid);
	void sync(local_pgid_t pgidStart, local_pgid_t pgidEnd);

	//! expand file size and mapped region
	void expandFile(size_t pgs);

	size_t numPagesReserved() const
	{
		return m_numPagesReserved;	
	}

	part_id_t partid() const
	{
		return m_partid;	
	}

	const std::string& filename() const
	{
		return m_filename;
	}

	void dumpStat() const;
	void discardFile();

	bool isFile() const
	{
		return ! m_filename.empty();	
	}
	
private:
	//! struct corresponding to mmap-ed region (linked list)
	struct Mapping
	{
		//! prev.pgidEnd <= pgid < pgidEnd is mapped
		page_id_t pgidEnd;

		char* offset;

		//! ptr to next mapping
		std::auto_ptr<Mapping> next;
	};

	MappedFile(part_id_t partid, const char* filename, int fd, int prot);

	//! mmap more pages (does NOT expand file size)
	void moreMMap(size_t pgs);

	//! partition id
	part_id_t m_partid;

	//! path of file responsible for this partition
	/*!
	 *	empty if not mapped to file (just mem)
	 */
	std::string m_filename;

	//! opened file descriptor
	int m_fd;

	//! prot passed to mmap(2)
	int m_prot;

	//! first mmap-ed region
	Mapping m_mapFirst;

	Mapping* getmLast();

	local_pgid_t m_numPagesReserved;
};

class PartitionedPageIO : public PageIO
{
public:
	//! C-tor
	/*!
	 *	@param dbprefix
	 *		ex. "/db/file/path/dbname"
	 */
	PartitionedPageIO(const char* dbprefix, ptnk_opts_t opts, int mode = 0644);

	~PartitionedPageIO();

	typedef pair<std::string, part_id_t> partfile_t;
	typedef std::vector<partfile_t> Vpartfile_t;

	//! scan for partitioned db files
	static void scanFiles(Vpartfile_t* files, const char* dbprefix);

	//! delete all db files for _dbprefix_
	static void drop(const char* dbprefix);

	page_id_t alignCompactionThreshold(page_id_t threshold) const;

	// ====== implements PageIO interface ======

	virtual pair<Page, page_id_t> newPage();

	virtual Page readPage(page_id_t pgid);
	virtual void sync(page_id_t pgid);
	virtual void syncRange(page_id_t pgidStart, page_id_t pgidEnd);

	virtual page_id_t getLastPgId() const;
	virtual local_pgid_t getPartLastLocalPgId(part_id_t ptid) const;

	virtual bool needInit() const;

	virtual void newPart(bool bForce = false);
	virtual void discardOldPages(page_id_t threshold);

	virtual void dumpStat() const;

	// ====== inspection funcs for test ======
	
	size_t numPartitions_() const;

private:
	//! open partitioned db files and populate m_parts
	void openFiles();

	//! add new partition
	MappedFile* addNewPartition_unsafe();

	void scanLastPgId();

	//! db prefix str. see C-tor param
	std::string m_dbprefix;

	//! db file creat(2) perm
	int m_mode;	

	//! opts passed to partition handler PageIOs
	ptnk_opts_t m_opts;

	bool m_needInit;

	boost::ptr_array<boost::nullable<MappedFile>, PTNK_PARTID_MAX+1> m_parts;

	boost::mutex m_mtxAlloc;

	//! active partition where new pages are created
	/*!
	 *	all other partitions are read-only
	 */
	MappedFile* m_active;

	//! next free m_active local pgid
	volatile local_pgid_t m_pgidLNext;

	//! first loaded partition id
	part_id_t m_partidFirst;

	//! last partition id
	part_id_t m_partidLast;
};

} // end of namespace ptnk

#endif // _ptnk_partitionedpageio_h_
