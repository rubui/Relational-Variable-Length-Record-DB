#include <cstdio>
#include <string>

#include <sys/stat.h>
#include <sys/types.h>

#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = NULL;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    // If the file already exists, error
    if (fileExists(fileName))
        return PFM_FILE_EXISTS;

    // Attempt to open the file for writing
    FILE *pFile = fopen(fileName.c_str(), "wb");
    // Return an error if we fail
    if (pFile == NULL)
        return PFM_OPEN_FAILED;

    fclose (pFile);
    return SUCCESS;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    // If file cannot be successfully removed, error
    if (remove(fileName.c_str()) != 0)
        return PFM_REMOVE_FAILED;

    return SUCCESS;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    // If this handle already has an open file, error
    if (fileHandle.getfd() != NULL)
        return PFM_HANDLE_IN_USE;

    // If the file doesn't exist, error
    if (!fileExists(fileName.c_str()))
        return PFM_FILE_DN_EXIST;

    // Open the file for reading/writing in binary mode
    FILE *pFile;
    pFile = fopen(fileName.c_str(), "rb+");
    // If we fail, error
    if (pFile == NULL)
        return PFM_OPEN_FAILED;

    fileHandle.setfd(pFile);

    return SUCCESS;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE *pFile = fileHandle.getfd();

    // If not an open file, error
    if (pFile == NULL)
        return 1;

    // Flush and close the file
    fclose(pFile);

    fileHandle.setfd(NULL);

    return SUCCESS;
}

// Check if a file already exists
bool PagedFileManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

    _fd = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (_fd == NULL)
        return -1;
    // If pageNum doesn't exist, error
    if (getNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return FH_READ_FAILED;

    readPageCounter++;
    return SUCCESS;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (_fd == NULL)
        return -1;
    // Check if the page exists
    if (getNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Seek to the start of the page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Write the page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        // Immediately commit changes to disk
        fflush(_fd);
        writePageCounter++;
        return SUCCESS;
    }
    
    return FH_WRITE_FAILED;
}


RC FileHandle::appendPage(const void *data)
{
    if (_fd == NULL)
        return -1;
    // Seek to the end of the file
    if (fseek(_fd, 0, SEEK_END))
        return FH_SEEK_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        fflush(_fd);
        appendPageCounter++;
        return SUCCESS;
    }
    return FH_WRITE_FAILED;
}


unsigned FileHandle::getNumberOfPages()
{
    if (_fd == NULL)
        return 0;
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_fd), &sb) != 0)
        // On error, return 0
        return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return sb.st_size / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount   = readPageCounter;
    writePageCount  = writePageCounter;
    appendPageCount = appendPageCounter;
    return SUCCESS;
}

void FileHandle::setfd(FILE *fd)
{
    _fd = fd;
}

FILE *FileHandle::getfd()
{
    return _fd;
}