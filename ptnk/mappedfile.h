#ifndef _ptnk_mappedfile_h_
#define _ptnk_mappedfile_h_

#include "page.h"

namespace ptnk
{

#if defined(__linux__) && defined(__x86_64__)
#define PTNK_MMAP_HINT (char*)0x600000000000
#else
// no hint
#define PTNK_MMAP_HINT NULL
#endif

constexpr size_t NUM_PAGES_ALLOC_ONCE = 256;

constexpr unsigned long PARTSIZEFILE_MAX = 1024 * 1024 * 1024; // 1GB
constexpr unsigned long PARTSIZEFILE_MIN = 128 * 1024 * 1024; // 128MB
constexpr unsigned long PARTSIZEFILE_PREALLOC_THRESHOLD = 64 * 1024 * 1024; // 64MB  FIXME!
constexpr unsigned long PARTSIZEFILE_PREALLOC = PARTSIZEFILE_PREALLOC_THRESHOLD * 1.5;

constexpr long NPAGES_PARTMAX = PARTSIZEFILE_MAX/PTNK_PAGE_SIZE;
constexpr long NPAGES_PREALLOC_THRESHOLD = PARTSIZEFILE_PREALLOC_THRESHOLD/PTNK_PAGE_SIZE;
constexpr long NPAGES_PREALLOC = PARTSIZEFILE_PREALLOC/PTNK_PAGE_SIZE;

// file sync strategies:
//   -t 100 -n 1000000 results:
//   ext2 @ sync_file_range	3.13 sec 
//   ext2 @ fdatasync		3.84 sec 
//   ext4 @ sync_file_range 36 sec
//   ext4 @ fdatasync		17 sec
#ifdef __linux__
#define PTNK_SYNC_FILE_RANGE
#endif
// #define PTNK_FDATASYNC

class MappedFile
{
public:
	static MappedFile* openExisting(part_id_t partid, const std::string& filename, ptnk_opts_t opts);
	static MappedFile* createNew(part_id_t partid, const std::string& filename, ptnk_opts_t opts, int mode);
	static MappedFile* createMem();
	~MappedFile();

	char* calcPtr(local_pgid_t pgid);
	void sync(local_pgid_t pgidStart, local_pgid_t pgidEnd);

	bool isReadOnly() const { return m_isReadOnly; }
	void makeReadOnly();

	//! alloc more pages by expanding filesize and mapped region
	/*!
	 *	@return number of actually alloced pages (0 if none alloced)
	 */
	size_t expandFile(size_t pgs);

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

	void discardFile();

	void dump(std::ostream& s) const;
	
private:
	//! struct corresponding to mmap-ed region (linked list)
	struct Mapping
	{
		//! prev.pgidEnd <= pgid < pgidEnd is mapped
		page_id_t pgidEnd;

		char* offset;

		//! ptr to next mapping
		unique_ptr<Mapping> next;
	};

	MappedFile(part_id_t partid, const std::string& filename, int fd, int prot);

	//! mmap more pages (does NOT expand file size)
	void moreMMap(size_t pgs);

	//! partition id
	part_id_t m_partid;

	//! path of file responsible for this partition
	/*!
	 *	empty if not mapped to file (just mem)
	 */
	std::string m_filename;

	//! true if not mapped to file (mapped to anon mem)
	bool m_bInMem;

	//! opened file descriptor
	int m_fd;

	//! prot passed to mmap(2)
	int m_prot;

	//! true if this part is mmap-ed read-only
	bool m_isReadOnly;

	//! first mmap-ed region
	Mapping m_mapFirst;

	Mapping* getmLast();

	local_pgid_t m_numPagesReserved;
};
inline
std::ostream& operator<<(std::ostream& s, const MappedFile& o)
{ o.dump(s); return s; }

inline
char*
MappedFile::calcPtr(local_pgid_t pgid)
{
	for(Mapping* p = &m_mapFirst; p; p = p->next.get())
	{
		if(PTNK_LIKELY(pgid < p->pgidEnd))
		{
			return p->offset + PTNK_PAGE_SIZE * pgid;
		}
	}

	PTNK_CHECK(false);
	return NULL;
}


} // end of namespace ptnk

#endif // _ptnk_mappedfile_h_
