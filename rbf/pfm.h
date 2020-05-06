#ifndef _pfm_h_
#define _pfm_h_

#define SUCCESS 0

#define PFM_FILE_EXISTS   1
#define PFM_OPEN_FAILED   2
#define PFM_REMOVE_FAILED 3
#define PFM_HANDLE_IN_USE 4
#define PFM_FILE_DN_EXIST 5
#define PFM_FILE_NOT_OPEN 6

#define FH_PAGE_DN_EXIST  1
#define FH_SEEK_FAILED    2
#define FH_READ_FAILED    3
#define FH_WRITE_FAILED   4

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                          // Create a new file
    RC destroyFile   (const string &fileName);                          // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);  // Open a file
    RC closeFile     (FileHandle &fileHandle);                          // Close a file

protected:
    PagedFileManager();                                                 // Constructor
    ~PagedFileManager();                                                // Destructor

private:
    static PagedFileManager *_pf_manager;

    // Private helper methods
    bool fileExists(const string &fileName);
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    
    FileHandle();                                                       // Default constructor
    ~FileHandle();                                                      // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables

    // Let PagedFileManager access our private helper methods
    friend class PagedFileManager;

private:
    FILE *_fd;

    // Private helper methods
    void setfd(FILE *fd);
    FILE *getfd();
}; 

#endif
