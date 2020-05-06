#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    //cout << recordSize <<endl;

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = getOpenSlot(pageData);

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    if (rid.slotNum == slotHeader.recordEntriesNumber)
        slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    SlotStatus status = getSlotStatus(recordEntry);
    switch (status)
    {
        // Error to read a deleted record
        case DEAD:
            free(pageData);
            return RBFM_READ_AFTER_DEL;
        // Get the forwarding address from the record entry and recurse
        case MOVED:
            free(pageData);
            RID newRid;
            newRid.pageNum = recordEntry.length;
            newRid.slotNum = -recordEntry.offset;
            return readRecord(fileHandle, recordDescriptor, newRid, data);
        // Retrieve the actual entry data
        case VALID:
            int32_t offset = recordEntry.offset;
            getRecordAtOffset(pageData, offset, recordDescriptor, data);
            free(pageData);
            return SUCCESS;
    }
    // Not possible to reach this point, but compiler doesn't know that
    return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    // Get page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData) != SUCCESS)
        return RBFM_READ_FAILED;

    // Get page header
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if (slotHeader.recordEntriesNumber <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Get slot record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    SlotStatus status = getSlotStatus(recordEntry);
    // Cannot delete a deleted page
    if (status == DEAD)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }
    // Recursively delete moved pages
    else if (status == MOVED)
    {
        RID newRid;
        newRid.pageNum = recordEntry.length;
        newRid.slotNum = -recordEntry.offset;
        RC rc = deleteRecord(fileHandle, recordDescriptor, newRid);
        if (rc != SUCCESS)
        {
            free(pageData);
            return rc;
        }
        markSlotDeleted(pageData, rid.slotNum);
    }
    else if (status == VALID)
    {
        markSlotDeleted(pageData, rid.slotNum);
        reorganizePage(pageData);
    }
    
    // Once we've deleted the page(s), write changes to disk
    RC rc = fileHandle.writePage(rid.pageNum, pageData);
    free(pageData);
    return rc;
}

// update record
// smaller: write at offset + size differece, update slot info, reorganize
// Larger but fits: remove, reorganize, setRecordAtOffset
// Larger dnf: remove, reorganize, insert into new page and update slot info
// same: do nothing
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    // Retrieve the specific page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
    {
        free(pageData);
        return RBFM_READ_FAILED;
    }

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber <= rid.slotNum)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    SlotStatus status = getSlotStatus(recordEntry);
    switch (status)
    {
        // Error to update a deleted record
        case DEAD:
            free(pageData);
            return RBFM_READ_AFTER_DEL;
        // Get the forwarding address from the record entry and recurse
        case MOVED:
            free(pageData);
            RID newRid;
            newRid.pageNum = recordEntry.length;
            newRid.slotNum = -recordEntry.offset;
            return updateRecord(fileHandle, recordDescriptor, data, newRid);
        default:
        break;
    }
    // Do actual work
    // Gets the size of the updated record
    unsigned recordSize = getRecordSize(recordDescriptor, data);
    if (recordSize  == recordEntry.length)
    {
        setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
        RC rc = fileHandle.writePage(rid.pageNum, pageData);
        free(pageData);
        return rc;
    }
    else if (recordSize < recordEntry.length)
    {
        setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
        recordEntry.length = recordSize;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        reorganizePage(pageData);
        RC rc = fileHandle.writePage(rid.pageNum, pageData);
        free(pageData);
        return rc;
    }
    else if (recordSize > recordEntry.length)
    {
        unsigned space = getPageFreeSpaceSize(pageData) + recordEntry.length;
        if (recordSize > space)
        {
            // Need to insert then set forward address then reorganize
            RID newRid;
            RC rc = insertRecord(fileHandle, recordDescriptor, data, newRid);
            if (rc != SUCCESS)
            {
                free(pageData);
                return rc;
            }
            recordEntry.length = newRid.pageNum;
            recordEntry.offset = -newRid.slotNum;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            reorganizePage(pageData);
        }
        else
        {
            // Need to set header to DEAD and reorganize to consolidate free space
            recordEntry.length = 0;
            recordEntry.offset = 0;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            reorganizePage(pageData);

            // Get updated slotHeader with new free space pointer
            slotHeader = getSlotDirectoryHeader(pageData);
            // Update record length and offset
            recordEntry.length = recordSize;
            recordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

            // Update header with new free space pointer
            slotHeader.freeSpaceOffset = recordEntry.offset;
            setSlotDirectoryHeader(pageData, slotHeader);

            // Add new record data
            setRecordAtOffset (pageData, recordEntry.offset, recordDescriptor, data);
        }
    }
    RC rc = fileHandle.writePage(rid.pageNum, pageData);
    free(pageData);
    return rc;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    char *pageData = (char*)malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(rid.pageNum, pageData) != SUCCESS)
    {
        free(pageData);
        return RBFM_READ_FAILED;
    }
    // Get record header, recurse if forwarded
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    SlotStatus status = getSlotStatus(recordEntry);
    switch (status)
    {
        // Error to get attribute of a deleted record
        case DEAD:
            free(pageData);
            return RBFM_READ_AFTER_DEL;
        // Get the forwarding address from the record entry and recurse
        case MOVED:
            free(pageData);
            RID newRid;
            newRid.pageNum = recordEntry.length;
            newRid.slotNum = -recordEntry.offset;
            return readAttribute(fileHandle, recordDescriptor, newRid, attributeName, data);
        default:
        break;
    }

    // Get offset to record
    unsigned offset = recordEntry.offset;
    // Get index and type of attribute
    auto pred = [&](Attribute a) {return a.name == attributeName;};
    auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
    unsigned index = distance(recordDescriptor.begin(), iterPos);
    if (index == recordDescriptor.size())
        return RBFM_NO_SUCH_ATTR;
    AttrType type = recordDescriptor[index].type;
    // Write attribute to data
    getAttributeFromRecord(pageData, offset, index, type, data);
    free(pageData);
    return SUCCESS;
}

// Scan returns an iterator to allow the caller to go through the results one by one. 
  RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
    return rbfm_ScanIterator.scanInit(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

RBFM_ScanIterator::RBFM_ScanIterator()
: currPage(0), currSlot(0), totalPage(0), totalSlot(0)
{
    rbfm = RecordBasedFileManager::instance();
}

RC RBFM_ScanIterator::close()
{
    free(pageData);
    return SUCCESS;
}

// Initialize the scanIterator with all necessary state
RC RBFM_ScanIterator::scanInit(FileHandle &fh,
        const vector<Attribute> rd,
        const string &ca, 
        const CompOp co, 
        const void *v, 
        const vector<string> &an)
{
    // Start at page 0 slot 0
    currPage = 0;
    currSlot = 0;
    totalPage = 0;
    totalSlot = 0;
    // Keep a buffer to hold the current page
    pageData = malloc(PAGE_SIZE);

    // Store the variables passed in to
    fileHandle = fh;
    conditionAttribute = ca;
    recordDescriptor = rd;
    compOp = co;
    value = v;
    attributeNames = an;

    skipList.clear();

    // Get total number of pages
    totalPage = fh.getNumberOfPages();
    if (totalPage > 0)
    {
        if (fh.readPage(0, pageData))
            return RBFM_READ_FAILED;
    }
    else
        return SUCCESS;

    // Get number of slots on first page
    SlotDirectoryHeader header = rbfm->getSlotDirectoryHeader(pageData);
    totalSlot = header.recordEntriesNumber;

    // If we don't need to do any comparisons, we can ignore the condition attribute
    if (co == NO_OP)
        return SUCCESS;

    // Else, we need to find the condition attribute's index in the record descriptor
    auto pred = [&](Attribute a) {return a.name == conditionAttribute;};
    auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
    attrIndex = distance(recordDescriptor.begin(), iterPos);
    if (attrIndex == recordDescriptor.size())
        return RBFM_NO_SUCH_ATTR;

    return SUCCESS;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    RC rc = getNextSlot();
    if (rc)
        return rc;

    // If we are not returning any results, we can just set the RID and return
    if (attributeNames.size() == 0)
    {
        rid.pageNum = currPage;
        rid.slotNum = currSlot++;
        return SUCCESS;
    }

    // Prepare null indicator
    unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(attributeNames.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currSlot);

    // Unsure how large each attribute will be, set to size of page to be safe
    void *buffer = malloc(PAGE_SIZE);
    if (buffer == NULL)
        return RBFM_MALLOC_FAILED;

    // Keep track of offset into data
    unsigned dataOffset = nullIndicatorSize;

    for (unsigned i = 0; i < attributeNames.size(); i++)
    {
        // Get index and type of attribute in record
        auto pred = [&](Attribute a) {return a.name == attributeNames[i];};
        auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
        unsigned index = distance(recordDescriptor.begin(), iterPos);
        if (index == recordDescriptor.size())
            return RBFM_NO_SUCH_ATTR;
        AttrType type = recordDescriptor[index].type;

        // Read attribute into buffer
        rbfm->getAttributeFromRecord(pageData, recordEntry.offset, index, type, buffer);
        // Determine if null
        char null;
        memcpy (&null, buffer, 1);
        if (null)
        {
            int indicatorIndex = i / CHAR_BIT;
            char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
            nullIndicator[indicatorIndex] |= indicatorMask;
        }
        // Read from buffer into data
        else if (type == TypeInt)
        {
            memcpy ((char*)data + dataOffset, (char*)buffer + 1, INT_SIZE);
            dataOffset += INT_SIZE;
        }
        else if (type == TypeReal)
        {
            memcpy ((char*)data + dataOffset, (char*)buffer + 1, REAL_SIZE);
            dataOffset += REAL_SIZE;
        }
        else if (type == TypeVarChar)
        {
            uint32_t varcharSize;
            memcpy(&varcharSize, (char*)buffer + 1, VARCHAR_LENGTH_SIZE);
            memcpy((char*)data + dataOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
            dataOffset += VARCHAR_LENGTH_SIZE;
            memcpy((char*)data + dataOffset, (char*)buffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
            dataOffset += varcharSize;
        }
    }
    // Finally set null indicator of data, clean up and return
    memcpy((char*)data, nullIndicator, nullIndicatorSize);

    free (buffer);
    rid.pageNum = currPage;
    rid.slotNum = currSlot++;
    return SUCCESS;
}

// Private helper methods ///////////////////////////////////////////////////////////////////

RC RBFM_ScanIterator::getNextSlot()
{
    // If we're done with the current page, or we've read the last page
    if (currSlot >= totalSlot || currPage >= totalPage)
    {
        // Reinitialize the current slot and increment page number
        currSlot = 0;
        currPage++;
        // If we're done with last page, return EOF
        if (currPage >= totalPage)
            return RBFM_EOF;
        // Otherwise get next page ready
        RC rc = getNextPage();
        if (rc)
            return rc;
    }

    // Get slot header, check to see if valid and meets scan condition
    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currSlot);

    if (rbfm->getSlotStatus(recordEntry) != VALID || !checkScanCondition())
    {
        // If not, try next slot
        currSlot++;
        return getNextSlot();
    }
    return SUCCESS;
}

RC RBFM_ScanIterator::getNextPage()
{
    // Read in page
    if (fileHandle.readPage(currPage, pageData))
        return RBFM_READ_FAILED;

    // Update slot total
    SlotDirectoryHeader header = rbfm->getSlotDirectoryHeader(pageData);
    totalSlot = header.recordEntriesNumber;
    return SUCCESS;
}

bool RBFM_ScanIterator::checkScanCondition()
{
    if (compOp == NO_OP) return true;
    if (value == NULL) return false;
    Attribute attr = recordDescriptor[attrIndex];
    // Allocate enough memory to hold attribute and 1 byte null indicator
    void *data = malloc(1 + attr.length);
    // Get record entry to get offset
    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currSlot);
    // Grab the given attribute and store it in data
    rbfm->getAttributeFromRecord(pageData, recordEntry.offset, attrIndex, attr.type, data);

    char null;
    memcpy(&null, data, 1);

    bool result = false;
    if (null)
    {
        result = false;
    }
    // Checkscan condition on record data and scan value
    else if (attr.type == TypeInt)
    {
        int32_t recordInt;
        memcpy(&recordInt, (char*)data + 1, INT_SIZE);
        result = checkScanCondition(recordInt, compOp, value);
    }
    else if (attr.type == TypeReal)
    {
        float recordReal;
        memcpy(&recordReal, (char*)data + 1, REAL_SIZE);
        result = checkScanCondition(recordReal, compOp, value);
    }
    else if (attr.type == TypeVarChar)
    {
        uint32_t varcharSize;
        memcpy(&varcharSize, (char*)data + 1, VARCHAR_LENGTH_SIZE);
        char recordString[varcharSize + 1];
        memcpy(recordString, (char*)data + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
        recordString[varcharSize] = '\0';

        result = checkScanCondition(recordString, compOp, value);
    }
    free (data);
    return result;
}

bool RBFM_ScanIterator::checkScanCondition(int recordInt, CompOp compOp, const void *value)
{
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool RBFM_ScanIterator::checkScanCondition(float recordReal, CompOp compOp, const void *value)
{
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool RBFM_ScanIterator::checkScanCondition(char *recordString, CompOp compOp, const void *value)
{
    if (compOp == NO_OP)
        return true;

    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int cmp = strcmp(recordString, valueStr);
    switch (compOp)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        // Should never happen
        default: return false;
    }
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

void RecordBasedFileManager::getRecordAtOffset(void *page, int32_t offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, (char*)page + offset, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), nullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

SlotStatus RecordBasedFileManager::getSlotStatus(SlotDirectoryRecordEntry slot)
{
    if (slot.length == 0 && slot.offset == 0)
        return DEAD;
    if (slot.offset <= 0)
        return MOVED;
    return VALID;
}

// Get first unused slot in page. Slot is considered unused if dead
// If not dead slots returns recordEntriesNumber
unsigned RecordBasedFileManager::getOpenSlot(void *page)
{
    SlotDirectoryHeader header = getSlotDirectoryHeader(page);
    unsigned i;
    for (i = 0; i < header.recordEntriesNumber; i++)
    {
        SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, i);
        SlotStatus status = getSlotStatus(recordEntry);
        if (status == DEAD)
            return i;
    }
    return i;
}

// Mark slot header as dead (all 0s)
void RecordBasedFileManager::markSlotDeleted(void *page, unsigned i)
{
    memset  (
            ((char*) page + sizeof(SlotDirectoryHeader) + i * sizeof(SlotDirectoryRecordEntry)),
            0,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Consolidates free space in center of page
void RecordBasedFileManager::reorganizePage(void *page)
{
    SlotDirectoryHeader header = getSlotDirectoryHeader(page);

    // Add all live records to vector, keeping track of slot numbers
    vector<IndexedRecordEntry> liveRecords;
    for (unsigned i = 0; i < header.recordEntriesNumber; i++)
    {
        IndexedRecordEntry entry;
        entry.slotNum = i;
        entry.recordEntry = getSlotDirectoryRecordEntry(page, i);
        if (getSlotStatus(entry.recordEntry) == VALID)
            liveRecords.push_back(entry);
    }
    // Sort records by offset, descending
    auto comp = [](IndexedRecordEntry first, IndexedRecordEntry second) 
        {return first.recordEntry.offset > second.recordEntry.offset;};
    sort(liveRecords.begin(), liveRecords.end(), comp);

    // Move each record back filling in any gap preceding the record
    uint16_t pageOffset = PAGE_SIZE;
    SlotDirectoryRecordEntry current;
    for (unsigned i = 0; i < liveRecords.size(); i++)
    {
        current = liveRecords[i].recordEntry;
        pageOffset -= current.length;

        // Use memmove rather than memcpy because locations may overlap
        memmove((char*)page + pageOffset, (char*)page + current.offset, current.length);
        current.offset = pageOffset;
        setSlotDirectoryRecordEntry(page, liveRecords[i].slotNum, current);
    }
    header.freeSpaceOffset = pageOffset;
    setSlotDirectoryHeader(page, header);
}

void RecordBasedFileManager::getAttributeFromRecord(void *page, unsigned offset, unsigned attrIndex, AttrType type, void *data)
{
    char *start = (char*)page + offset;
    unsigned data_offset = 0;

    // Get number of columns
    RecordLength n;
    memcpy (&n, start, sizeof(RecordLength));

    // Get null indicator
    int recordNullIndicatorSize = getNullIndicatorSize(n);
    char recordNullIndicator[recordNullIndicatorSize];
    memcpy (recordNullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // Set null indicator for result
    char resultNullIndicator = 0;
    if (fieldIsNull(recordNullIndicator, attrIndex))
        resultNullIndicator |= (1 << 7);
    memcpy(data, &resultNullIndicator, 1);
    data_offset += 1;
    if (resultNullIndicator) return;

    // Now we know the result isn't null, so we grab it
    unsigned header_offset = sizeof(RecordLength) + recordNullIndicatorSize;
    // attrEnd points to end of attribute, attrStart points to the beginning
    // Our directory at the beginning of each record contains pointers to the ends of each attribute,
    // so we can pull attrEnd from that
    ColumnOffset attrEnd, attrStart;
    memcpy(&attrEnd, start + header_offset + attrIndex * sizeof(ColumnOffset), sizeof(ColumnOffset));
    // The start is either the end of the previous attribute, or the start of the data section of the
    // record if we are after the 0th attribute
    if (attrIndex > 0)
        memcpy(&attrStart, start + header_offset + (attrIndex - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
    else
        attrStart = header_offset + n * sizeof(ColumnOffset);
    // The length of any attribute is just the difference between its start and end
    uint32_t len = attrEnd - attrStart;
    if (type == TypeVarChar)
    {
        // For varchars we have to return this length in the result
        memcpy((char*)data + data_offset, &len, sizeof(VARCHAR_LENGTH_SIZE));
        data_offset += VARCHAR_LENGTH_SIZE;
    }
    // For all types, we then copy the data into the result
    memcpy((char*)data + data_offset, start + attrStart, len);
}