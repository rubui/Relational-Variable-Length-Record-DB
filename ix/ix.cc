
#include "ix.h"

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"

#include <vector>
#include <string>
#include <cstring>
#include <iostream>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();

    if (pfm->createFile(fileName.c_str()))
        return IX_CREATE_FAILED;

    // Open the file we just created
    IXFileHandle handle;
    RC rc = openFile(fileName, handle);
    if (rc)
        return IX_OPEN_FAILED;

    void *pageData = calloc(PAGE_SIZE, 1);
    if (pageData == NULL)
        return IX_MALLOC_FAILED;

    // Initialize the first page with metadata. root page will be page 1
    MetaHeader meta;
    meta.rootPage = 1;
    setMetaData(meta, pageData);
    rc = handle.appendPage(pageData);
    if (rc)
    {
        closeFile(handle);
        free(pageData);
        return IX_APPEND_FAILED;
    }

    // Initialize root page as internal node with a single child
    setNodeType(IX_TYPE_INTERNAL, pageData);
    InternalHeader header;
    header.entriesNumber = 0;
    header.freeSpaceOffset = PAGE_SIZE;
    header.leftChildPage = 2;
    setInternalHeader(header, pageData);
    rc = handle.appendPage(pageData);
    if (rc)
    {
        closeFile(handle);
        free(pageData);
        return IX_APPEND_FAILED;
    }

    // Set up child of root as empty leaf
    setNodeType(IX_TYPE_LEAF, pageData);
    LeafHeader leafHeader;
    leafHeader.next            = 0;
    leafHeader.prev            = 0;
    leafHeader.entriesNumber   = 0;
    leafHeader.freeSpaceOffset = PAGE_SIZE;
    setLeafHeader(leafHeader, pageData);
    rc = handle.appendPage(pageData);
    if (rc)
    {
        closeFile(handle);
        free(pageData);
        return IX_APPEND_FAILED;
    }

    closeFile(handle);
    free(pageData);
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->destroyFile(fileName))
        return IX_DESTROY_FAILED;
    return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->openFile(fileName, ixfileHandle.fh))
        return IX_OPEN_FAILED;
    return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->closeFile(ixfileHandle.fh))
        return IX_CLOSE_FAILED;
    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

    ChildEntry childEntry = {.key = NULL, .childPage = 0};
    int32_t rootPage;
    RC rc = getRootPageNum(ixfileHandle, rootPage);
    if (rc)
        return rc;
    return insert(attribute, key, rid, ixfileHandle, rootPage, childEntry);
}

RC IndexManager::insert(const Attribute &attribute, const void *key, const RID &rid, IXFileHandle &fileHandle, int32_t pageID, ChildEntry &childEntry)
{
    void *pageData = malloc(PAGE_SIZE);
    if(pageData == NULL)
        return IX_MALLOC_FAILED;
    if (fileHandle.readPage(pageID, pageData))
    {
        free(pageData);
        return IX_READ_FAILED;
    }

    NodeType type = getNodetype(pageData);

    if (type == IX_TYPE_INTERNAL)
    {
        int32_t childPage = getNextChildPage(attribute, key, pageData);
        free (pageData);
        if (childPage == 0)
            return IX_BAD_CHILD;

        // Recursively insert
        RC rc = insert(attribute, key, rid, fileHandle, childPage, childEntry);
        if (rc)
            return rc;
        if(childEntry.key == NULL)
            return SUCCESS;
        // If we're here, we need to handle a split
        pageData = malloc(PAGE_SIZE);
        if (fileHandle.readPage(pageID, pageData))
        {
            free(pageData);
            return IX_READ_FAILED;
        }

        rc = insertIntoInternal(attribute, childEntry, pageData);
        if (rc == SUCCESS)
        {
            rc = fileHandle.writePage(pageID, pageData);

            // If childEntry contains any data, we clear it to defaults
            free (childEntry.key);
            childEntry.key = NULL;
            childEntry.childPage = 0;
            free(pageData);
            pageData = NULL;
            // If write succeeded, rc is success, otherwise it's a failure.
            return rc == SUCCESS ? SUCCESS : IX_WRITE_FAILED;
        }
        else if (IX_NO_FREE_SPACE)
        {
            rc = splitInternal(fileHandle, attribute, pageID, pageData, childEntry);
            free(pageData);
            pageData = NULL;
            return rc;
        }
        else // Some other error, probably will not occur
        {
            free(pageData);
            free(childEntry.key);
            childEntry.key = NULL;
            return IX_INSERT_INTERNAL_FAILED;
        }

    }
    else // This is a leaf node
    {
        // Try to insert
        RC rc = insertIntoLeaf(attribute, key, rid, pageData);
        if (rc == SUCCESS) // We managed to insert the new pair into this leaf.
        {
            // Write our changes
            if (fileHandle.writePage(pageID, pageData))
                return IX_WRITE_FAILED;

            // If childEntry contains any data, we clear it to defaults
            free (childEntry.key);
            childEntry.key = NULL;
            childEntry.childPage = 0;

            free(pageData);
            pageData = NULL;
            return SUCCESS;
        }
        else if (rc == IX_NO_FREE_SPACE) // Leaf is full and needs to be split
        {
            rc = splitLeaf(fileHandle, attribute, key, rid, pageID, pageData, childEntry);
            free(pageData);
            pageData = NULL;
            return rc;
        }
        else // Some other error, probably will not occur
        {
            free(pageData);
            pageData = NULL;
            free(childEntry.key);
            childEntry.key = NULL;
            return IX_INSERT_LEAF_FAILED;
        }
    }
}

RC IndexManager::splitLeaf(IXFileHandle &fileHandle, const Attribute &attribute, const void *ins_key, const RID ins_rid, const int32_t pageID, void *originalLeaf, ChildEntry &childEntry)
{
    LeafHeader originalHeader = getLeafHeader(originalLeaf);

    // Create new leaf to hold overflow
    void *newLeaf = calloc(PAGE_SIZE, 1);
    setNodeType(IX_TYPE_LEAF, newLeaf);
    LeafHeader newHeader;
    newHeader.prev = pageID;
    newHeader.next = originalHeader.next;
    newHeader.entriesNumber = 0;
    newHeader.freeSpaceOffset = PAGE_SIZE;
    setLeafHeader(newHeader, newLeaf);

    int32_t newPageNum = fileHandle.getNumberOfPages();
    originalHeader.next = newPageNum;
    setLeafHeader(originalHeader, originalLeaf);

    int size = 0;
    int i;
    int lastSize = 0;
    for (i = 0; i < originalHeader.entriesNumber; i++)
    {
        DataEntry entry = getDataEntry(i, originalLeaf);
        void *key = NULL;

        if (attribute.type == TypeInt)
            key = &(entry.integer);
        else if (attribute.type == TypeReal)
            key = &(entry.real);
        else
            key = (char*)originalLeaf + entry.varcharOffset;

        lastSize = getKeyLengthLeaf(attribute, key);
        size += lastSize;
        if (size >= PAGE_SIZE / 2)
        {
            if (i >= originalHeader.entriesNumber - 1 || compareLeafSlot(attribute, key, originalLeaf, i+1) != 0)
                break;
        }
    }
    // i is now middle key
    DataEntry middleEntry = getDataEntry(i, originalLeaf);
    childEntry.key = malloc(lastSize);
    if (childEntry.key == NULL)
        return IX_MALLOC_FAILED;
    childEntry.childPage = newPageNum;
    int keySize = attribute.type == TypeVarChar ? lastSize - sizeof(DataEntry) : INT_SIZE;
    if (attribute.type == TypeVarChar)
        memcpy(childEntry.key, (char*)originalLeaf + middleEntry.varcharOffset, keySize);
    else
        memcpy(childEntry.key, &(middleEntry.integer), keySize);

    void *moving_key = malloc (attribute.length + 4);
    for (int j = 1; j < originalHeader.entriesNumber - i; j++)
    {
        // Grab data entry after the middle entry. We then delete it and the rest are shifted over
        DataEntry entry = getDataEntry(i + 1, originalLeaf);
        RID moving_rid = entry.rid;
        if (attribute.type == TypeVarChar)
        {
            int32_t len;
            memcpy(&len, (char*)originalLeaf + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
            memcpy(moving_key, &len, VARCHAR_LENGTH_SIZE);
            memcpy((char*)moving_key + VARCHAR_LENGTH_SIZE, (char*)originalLeaf + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
        }
        else
        {
            memcpy(moving_key, &(entry.integer), INT_SIZE);
        }
        // Insert into new leaf, delete from old
        insertIntoLeaf(attribute, moving_key, moving_rid, newLeaf);
        deleteEntryFromLeaf(attribute, moving_key, moving_rid, originalLeaf);
    }
    free(moving_key);

    // Still need to: Write back both (append new leaf)
    // Add new record to correct page
    if (compareLeafSlot(attribute, ins_key, originalLeaf, i) <= 0)
    {
        if (insertIntoLeaf(attribute, ins_key, ins_rid, originalLeaf))
        {
            free(newLeaf);
            return -1;
        }
    }
    else
    {
        if (insertIntoLeaf(attribute, ins_key, ins_rid, newLeaf))
        {
            free(newLeaf);
            return -1;
        }
    }

    if(fileHandle.writePage(pageID, originalLeaf))
    {
        free(newLeaf);
        return IX_WRITE_FAILED;
    }
    if(fileHandle.appendPage(newLeaf))
    {
        free(newLeaf);
        return IX_APPEND_FAILED;
    }
    free(newLeaf);
    return SUCCESS;
}

RC IndexManager::insertIntoInternal(const Attribute attribute, ChildEntry entry, void *pageData)
{
    InternalHeader header = getInternalHeader(pageData);
    int len = getKeyLengthInternal(attribute, entry.key);

    if (getFreeSpaceInternal(pageData) < len)
        return IX_NO_FREE_SPACE;

    int i;
    for (i = 0; i < header.entriesNumber; i++)
    {
        if (compareSlot(attribute, entry.key, pageData, i) <= 0)
            break;
    }

    // i is slot number where new entry will go
    // i is slot number to move
    int start_offset = getOffsetOfInternalSlot(i);
    int end_offset = getOffsetOfInternalSlot(header.entriesNumber);
    // Shift everything starting at start_offset to the right to make room for a new dataEntry
    memmove((char*)pageData + start_offset + sizeof(IndexEntry), (char*)pageData + start_offset, end_offset - start_offset);

    IndexEntry newEntry;
    newEntry.childPage = entry.childPage;
    if (attribute.type == TypeInt)
        memcpy(&newEntry.integer, entry.key, INT_SIZE);
    else if (attribute.type == TypeReal)
        memcpy(&newEntry.real, entry.key, REAL_SIZE);
    else
    {
        int32_t len;
        memcpy(&len, entry.key, VARCHAR_LENGTH_SIZE);
        newEntry.varcharOffset = header.freeSpaceOffset - (len + VARCHAR_LENGTH_SIZE);
        memcpy((char*)pageData + newEntry.varcharOffset, entry.key, len + VARCHAR_LENGTH_SIZE);
        header.freeSpaceOffset = newEntry.varcharOffset;
    }
    header.entriesNumber += 1;
    setInternalHeader(header, pageData);
    setIndexEntry(newEntry, i, pageData);
    return SUCCESS;
}

RC IndexManager::insertIntoLeaf(const Attribute attribute, const void *key, const RID &rid, void *pageData)
{
    LeafHeader header = getLeafHeader(pageData);

    int32_t key_len = getKeyLengthLeaf(attribute, key);
    if (getFreeSpaceLeaf(pageData) < key_len)
        return IX_NO_FREE_SPACE;

    int i;
    for (i = 0; i < header.entriesNumber; i++)
    {
        if (compareLeafSlot(attribute, key, pageData, i) < 0)
            break;
    }

    // i is slot number to move
    int start_offset = getOffsetOfLeafSlot(i);
    int end_offset = getOffsetOfLeafSlot(header.entriesNumber);
    // Shift everything starting at start_offset to the right to make room for a new dataEntry
    memmove((char*)pageData + start_offset + sizeof(DataEntry), (char*)pageData + start_offset, end_offset - start_offset);

    DataEntry newEntry;
    newEntry.rid = rid;
    if (attribute.type == TypeInt)
        memcpy(&(newEntry.integer), key, INT_SIZE);
    else if (attribute.type == TypeReal)
        memcpy(&(newEntry.real), key, REAL_SIZE);
    else
    {
        int32_t len;
        memcpy(&len, key, VARCHAR_LENGTH_SIZE);
        newEntry.varcharOffset = header.freeSpaceOffset - (len + VARCHAR_LENGTH_SIZE);
        memcpy((char*)pageData + newEntry.varcharOffset, key, len + VARCHAR_LENGTH_SIZE);
        header.freeSpaceOffset = newEntry.varcharOffset;
    }
    header.entriesNumber += 1;
    setLeafHeader(header, pageData);
    setDataEntry(newEntry, i, pageData);
    return SUCCESS;
}

int IndexManager::getOffsetOfLeafSlot(int slotNum) const
{
    return sizeof(NodeType) + sizeof(LeafHeader) + slotNum * sizeof(DataEntry);
}

int IndexManager::getOffsetOfInternalSlot(int slotNum) const
{
    return sizeof(NodeType) + sizeof(InternalHeader) + slotNum * sizeof(IndexEntry);
}

RC IndexManager::splitInternal(IXFileHandle &fileHandle, const Attribute &attribute, const int32_t pageID, void *original, ChildEntry &childEntry)
{
    InternalHeader originalHeader = getInternalHeader(original);

    int32_t newPageNum = fileHandle.getNumberOfPages();

    int size = 0;
    int i;
    int lastSize = 0;
    for (i = 0; i < originalHeader.entriesNumber; i++)
    {
        IndexEntry entry = getIndexEntry(i, original);
        void *key;

        if (attribute.type == TypeInt)
            key = &(entry.integer);
        else if (attribute.type == TypeReal)
            key = &(entry.real);
        else
            key = (char*)original + entry.varcharOffset;

        lastSize = getKeyLengthInternal(attribute, key);
        size += lastSize;
        if (size >= PAGE_SIZE / 2)
        {
            break;
        }
    }
    // i is now middle key
    IndexEntry middleEntry = getIndexEntry(i, original);

    // Create new leaf to hold overflow
    void *newIntern = calloc(PAGE_SIZE, 1);
    setNodeType(IX_TYPE_INTERNAL, newIntern);
    InternalHeader newHeader;
    newHeader.entriesNumber = 0;
    newHeader.freeSpaceOffset = PAGE_SIZE;
    newHeader.leftChildPage = middleEntry.childPage;
    setInternalHeader(newHeader, newIntern);

    // Get size of middle key, and store middle key for later
    int keySize = attribute.type == TypeVarChar ? lastSize - sizeof(IndexEntry) : INT_SIZE;
    void *middleKey = malloc(keySize);
    if (attribute.type == TypeVarChar)
        memcpy(middleKey, (char*)original + middleEntry.varcharOffset, keySize);
    else
        memcpy(middleKey, &(middleEntry.integer), INT_SIZE);

    // Create storage for shifting keys from one page to the other
    void *moving_key = malloc (attribute.length + 4);
    // Repeatedly insert an entry from one page into the other, then delete the entry from the original page
    for (int j = 1; j < originalHeader.entriesNumber - i; j++)
    {
        // Grab data entry after the middle entry. We then delete it and the rest are shifted over
        IndexEntry entry = getIndexEntry(i + 1, original);
        int32_t moving_pagenum = entry.childPage;
        if (attribute.type == TypeVarChar)
        {
            int32_t len;
            memcpy(&len, (char*)original + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
            memcpy(moving_key, &len, VARCHAR_LENGTH_SIZE);
            memcpy((char*)moving_key + VARCHAR_LENGTH_SIZE,(char*)original + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
        }
        else
        {
            memcpy(moving_key, &(entry.integer), INT_SIZE);
        }
        ChildEntry tmp;
        tmp.key = moving_key;
        tmp.childPage = moving_pagenum;
        insertIntoInternal(attribute, tmp, newIntern);
        deleteEntryFromInternal(attribute, moving_key, original);
    }
    free(moving_key);
    // Delete middle entry
    deleteEntryFromInternal(attribute, middleKey, original);

    // If new key is less than middle key, put it in original node, else put it in new node
    if (compareSlot(attribute, childEntry.key, original, i) < 0)
    {
        if (insertIntoInternal(attribute, childEntry, original))
        {
            free(newIntern);
            return -1;
        }
    }
    else
    {
        if (insertIntoInternal(attribute, childEntry, newIntern))
        {
            free(newIntern);
            return -1;
        }
    }

    if(fileHandle.writePage(pageID, original))
    {
        free(newIntern);
        return IX_WRITE_FAILED;
    }
    if(fileHandle.appendPage(newIntern))
    {
        free(newIntern);
        return IX_APPEND_FAILED;
    }
    free(newIntern);

    // Take the key of middle entry and allow it to propogate up
    free(childEntry.key);
    childEntry.key = middleKey;
    childEntry.childPage = newPageNum;

    // Check if we're root, then handle that case if we are
    int32_t rootPage;
    getRootPageNum(fileHandle, rootPage);
    if (pageID == rootPage)
    {
        // Create new page and set appropriate headers
        void *newRoot = calloc(PAGE_SIZE, 1);

        setNodeType(IX_TYPE_INTERNAL, newRoot);
        InternalHeader rootHeader;
        rootHeader.entriesNumber = 0;
        rootHeader.freeSpaceOffset = PAGE_SIZE;
        // Left most will be the smaller of these two pages
        rootHeader.leftChildPage = pageID;
        setInternalHeader(rootHeader, newRoot);
        // Insert larger of these two pages after
        insertIntoInternal(attribute, childEntry, newRoot);

        // Update metadata page
        int newRootPage = fileHandle.getNumberOfPages();
        if(fileHandle.appendPage(newRoot))
            return IX_APPEND_FAILED;
        MetaHeader metahead;
        metahead.rootPage = newRootPage;
        setMetaData(metahead, newRoot);
        if(fileHandle.writePage(0, newRoot))
            return IX_WRITE_FAILED;
        // Free memory
        free(newRoot);
        free(childEntry.key);
        childEntry.key = NULL;

    }

    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int32_t leafPage;
    RC rc = find(ixfileHandle, attribute, key, leafPage);
    if (rc)
        return rc;
    // leafPage is page number of leaf where this entry would be
    // Read in page
    void *pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.readPage(leafPage, pageData))
    {
        free(pageData);
        return IX_READ_FAILED;
    }

    // Delete it from pageData
    rc = deleteEntryFromLeaf(attribute, key, rid, pageData);
    if (rc)
    {
        free(pageData);
        return rc;
    }

    rc = ixfileHandle.writePage(leafPage, pageData);
    free(pageData);
    return rc;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return ix_ScanIterator.initialize(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    int32_t rootPage;
    getRootPageNum(ixfileHandle, rootPage);

    cout << "{";
    printBtree_rec(ixfileHandle, "  ",rootPage, attribute);
    cout << endl << "}" << endl;
}

// Print comma from calling context.
void IndexManager::printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const
{
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(currPage, pageData);

    NodeType type = getNodetype(pageData);
    if (type == IX_TYPE_LEAF)
    {
        printLeafNode(pageData, attr);
    }
    else
    {
        printInternalNode(ixfileHandle, pageData, attr, prefix);
    }
    free(pageData);
}

void IndexManager::printInternalNode(IXFileHandle &ixfileHandle, void *pageData, const Attribute &attr, string prefix) const
{
    InternalHeader header = getInternalHeader(pageData);

    cout << "\n" << prefix << "\"keys\":[";
    for (int i = 0; i < header.entriesNumber; i++)
    {
        if (i != 0)
            cout << ",";
        printInternalSlot(attr, i, pageData);
    }
    cout << "],\n" << prefix << "\"children\":[\n" << prefix;

    for (int i = 0; i <= header.entriesNumber; i++)
    {
        if (i == 0)
        {
            cout << "{";
            printBtree_rec(ixfileHandle, prefix + "  ", header.leftChildPage, attr);
            cout << "}";
        }
        else
        {
            cout << ",\n" << prefix;
            IndexEntry entry = getIndexEntry(i - 1, pageData);
            cout << "{";
            printBtree_rec(ixfileHandle, prefix + "  ", entry.childPage, attr);
            cout << "}";
        }
    }
    cout << "\n" << prefix << "]";
}

void IndexManager::printLeafNode(void *pageData, const Attribute &attr) const
{
    LeafHeader header = getLeafHeader(pageData);
    void *key = NULL;
    if (attr.type != TypeVarChar)
        key = malloc (INT_SIZE);
    bool first = true;
    vector<RID> key_rids;

    cout << "\"keys\":[";

    for (int i = 0; i <= header.entriesNumber; i++)
    {
        DataEntry entry = getDataEntry(i, pageData);
        if (first && i < header.entriesNumber)
        {
            key_rids.clear();
            first = false;
            if (attr.type == TypeInt)
                memcpy(key, &(entry.integer), INT_SIZE);
            else if (attr.type == TypeReal)
                memcpy(key, &(entry.real), REAL_SIZE);
            else
            {
                // Deal with reading in varchar
                int len;
                memcpy(&len, (char*)pageData + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
                free(key);
                key = malloc(len + VARCHAR_LENGTH_SIZE + 1);
                memcpy(key, &len, VARCHAR_LENGTH_SIZE);
                memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)pageData + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
                memset((char*)key + VARCHAR_LENGTH_SIZE + len, 0, 1);
            }
        }
        if ( i < header.entriesNumber && compareLeafSlot(attr, key, pageData, i) == 0)
        {
            key_rids.push_back(entry.rid);
        }
        else if (i != 0)
        {
            cout << "\"";
            if (attr.type == TypeInt)
            {
                cout << "" << *(int*)key;
                memcpy(key, &(entry.integer), INT_SIZE);
            }
            else if (attr.type == TypeReal)
            {
                cout << "" << *(float*)key;
                memcpy(key, &(entry.real), REAL_SIZE);
            }
            else
            {
                cout << (char*)key + 4;

                int len;
                memcpy(&len, (char*)pageData + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
                free(key);
                key = malloc(len + VARCHAR_LENGTH_SIZE + 1);
                memcpy(key, &len, VARCHAR_LENGTH_SIZE);
                memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)pageData + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
                memset((char*)key + VARCHAR_LENGTH_SIZE + len, 0, 1);
            }

            cout << ":[";
            for (unsigned j = 0; j < key_rids.size(); j++)
            {
                if (j != 0)
                {
                    cout << ",";
                }
                cout << "(" << key_rids[j].pageNum << "," << key_rids[j].slotNum << ")";
            }
            cout << "]\"";
            key_rids.clear();
            key_rids.push_back(entry.rid);
        }
    }
    cout << "]}";
    free (key);
}

void IndexManager::printInternalSlot(const Attribute &attr, const int32_t slotNum, const void *data) const
{
    IndexEntry entry = getIndexEntry(slotNum, data);
    if (attr.type == TypeInt)
        cout << "" << entry.integer;
    else if (attr.type == TypeReal)
        cout << "" << entry.real;
    else
    {
        int32_t len;
        memcpy(&len, (char*)data + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
        char varchar[len + 1];
        varchar[len] = '\0';
        memcpy(varchar, (char*)data + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
        cout << "" << varchar;
    }
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::initialize(IXFileHandle &fh, Attribute attribute, const void *low, const void *high, bool lowInc, bool highInc)
{
    // Store all parameters because we will need them later
    attr = attribute;
    fileHandle = &fh;
    lowKey = low;
    highKey = high;
    lowKeyInclusive = lowInc;
    highKeyInclusive = highInc;

    // Initialize our storage
    page = malloc(PAGE_SIZE);
    if (page == NULL)
        return IX_MALLOC_FAILED;
    // Initialize starting slot number
    slotNum = 0;

    // Find the starting page
    IndexManager *im = IndexManager::instance();
    int32_t startPageNum;
    RC rc = im->find(*fileHandle, attr, lowKey, startPageNum);
    if (rc)
    {
        // cout << "initialize" << endl;
        free(page);
        return rc;
    }
    rc = fileHandle->readPage(startPageNum, page);
    if (rc)
    {
        free(page);
        return rc;
    }

    // Find the starting entry
    LeafHeader header = im->getLeafHeader(page);
    int i = 0;
    for (i = 0; i < header.entriesNumber; i++)
    {
        int cmp = (low == NULL ? -1 : im->compareLeafSlot(attr, lowKey, page, i));
        if (cmp < 0){
            // cout << "init < " <<  endl;
            break;
        }
        if (cmp == 0 && lowKeyInclusive){
            // cout << "init = " <<  endl;
            break;
        }
        if (cmp > 0){
            // cout << "init > " <<  endl;
            continue;
        }
    }
    slotNum = i;
    return SUCCESS;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    IndexManager *im = IndexManager::instance();
    LeafHeader header = im->getLeafHeader(page);
    // If we have run off the end of the page, jump to the next one
    if (slotNum >= header.entriesNumber)
    {
        // If there is no next page, return EOF
        if (header.next == 0)
            return IX_EOF;
        slotNum = 0;
        fileHandle->readPage(header.next, page);
        return getNextEntry(rid, key);
    }
    // If highkey is null, always carry on
    // Otherwise, carry on only if highkey is greater than the current key
    int cmp = highKey == NULL ? 1 : im->compareLeafSlot(attr, highKey, page, slotNum);
    if (cmp == 0 && !highKeyInclusive){
        // cout << "Not high inclusive" << endl;
        return IX_EOF;
    }
    if (cmp < 0){
        // cout << "cmp < 0" << endl;
        return IX_EOF;
    }

    // Grab the data entry, grab its rid
    DataEntry entry = im->getDataEntry(slotNum, page);
    rid.pageNum = entry.rid.pageNum;
    rid.slotNum = entry.rid.slotNum;
    // grab its key
    if (attr.type == TypeInt)
        memcpy(key, &(entry.integer), INT_SIZE);
    else if (attr.type == TypeReal)
        memcpy(key, &(entry.real), REAL_SIZE);
    else
    {
        int len;
        memcpy(&len, (char*)page + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
        memcpy(key, &len, VARCHAR_LENGTH_SIZE);
        memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)page + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
    }
    // increment slotNum for the next call to getNextEntry
    slotNum++;
    return SUCCESS;
}

RC IX_ScanIterator::close()
{
    free(page);
    return SUCCESS;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    ixReadPageCounter++;
    return fh.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    ixWritePageCounter++;
    return fh.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data)
{
    ixAppendPageCounter++;
    return fh.appendPage(data);
}

unsigned IXFileHandle::getNumberOfPages()
{
    return fh.getNumberOfPages();
}

// Private helpers -----------------------

void IndexManager::setMetaData(const MetaHeader header, void *pageData)
{
    memcpy(pageData, &header, sizeof(MetaHeader));
}

MetaHeader IndexManager::getMetaData(const void *pageData) const
{
    MetaHeader header;
    memcpy(&header, pageData, sizeof(MetaHeader));
    return header;
}

void IndexManager::setNodeType(const NodeType type, void *pageData)
{
    memcpy(pageData, &type, sizeof(NodeType));
}

NodeType IndexManager::getNodetype(const void *pageData) const
{
    NodeType result;
    memcpy(&result, pageData, sizeof(NodeType));
    return result;
}

void IndexManager::setInternalHeader(const InternalHeader header, void *pageData)
{
    const unsigned offset = sizeof(NodeType);
    memcpy((char*)pageData + offset, &header, sizeof(InternalHeader));
}

InternalHeader IndexManager::getInternalHeader(const void *pageData) const
{
    const unsigned offset = sizeof(NodeType);
    InternalHeader header;
    memcpy(&header, (char*)pageData + offset, sizeof(InternalHeader));
    return header;
}

void IndexManager::setLeafHeader(const LeafHeader header, void *pageData)
{
    const unsigned offset = sizeof(NodeType);
    memcpy((char*)pageData + offset, &header, sizeof(LeafHeader));
}

LeafHeader IndexManager::getLeafHeader(const void *pageData) const
{
    const unsigned offset = sizeof(NodeType);
    LeafHeader header;
    memcpy(&header, (char*)pageData + offset, sizeof(LeafHeader));
    return header;
}

void IndexManager::setIndexEntry(const IndexEntry entry, const int slotNum, void *pageData)
{
    const unsigned offset = sizeof(NodeType) + sizeof(InternalHeader);
    unsigned slotOffset = offset + slotNum * sizeof(IndexEntry);
    memcpy((char*) pageData + slotOffset, &entry, sizeof(IndexEntry));
}

IndexEntry IndexManager::getIndexEntry(const int slotNum, const void *pageData) const
{
    const unsigned offset = sizeof(NodeType) + sizeof(InternalHeader);
    unsigned slotOffset = offset + slotNum * sizeof(IndexEntry);
    IndexEntry entry;
    memcpy(&entry, (char*)pageData + slotOffset, sizeof(IndexEntry));
    return entry;
}

void IndexManager::setDataEntry(const DataEntry entry, const int slotNum, void *pageData)
{
    const unsigned offset = sizeof(NodeType) + sizeof(LeafHeader);
    unsigned slotOffset = offset + slotNum * sizeof(DataEntry);
    memcpy((char*) pageData + slotOffset, &entry, sizeof(DataEntry));
}

DataEntry IndexManager::getDataEntry(const int slotNum, const void *pageData) const
{
    const unsigned offset = sizeof(NodeType) + sizeof(LeafHeader);
    unsigned slotOffset = offset + slotNum * sizeof(DataEntry);
    DataEntry entry;
    memcpy(&entry, (char*)pageData + slotOffset, sizeof(DataEntry));
    return entry;
}

RC IndexManager::getRootPageNum(IXFileHandle &fileHandle, int32_t &result) const
{
    void *metaPage = malloc(PAGE_SIZE);
    if (metaPage == NULL)
        return IX_MALLOC_FAILED;
    RC rc = fileHandle.readPage(0, metaPage);
    if (rc)
    {
        free(metaPage);
        return IX_READ_FAILED;
    }

    MetaHeader header = getMetaData(metaPage);
    free(metaPage);
    result = header.rootPage;
    return SUCCESS;
}

RC IndexManager::find(IXFileHandle &handle, const Attribute attr, const void *key, int32_t &resultPageNum)
{
    int32_t rootPageNum;
    RC rc = getRootPageNum(handle, rootPageNum);
    if (rc)
        return rc;
    return treeSearch(handle, attr, key, rootPageNum, resultPageNum);
}

RC IndexManager::treeSearch(IXFileHandle &handle, const Attribute attr, const void *key, const int32_t currPageNum, int32_t &resultPageNum)
{
    void *pageData = malloc(PAGE_SIZE);

    if (handle.readPage(currPageNum, pageData))
    {
        free (pageData);
        return IX_READ_FAILED;
    }

    // Found our leaf!
    if (getNodetype(pageData) == IX_TYPE_LEAF)
    {
        resultPageNum = currPageNum;
        free(pageData);
        return SUCCESS;
    }

    int32_t nextChildPage = getNextChildPage(attr, key, pageData);

    free(pageData);
    return treeSearch(handle, attr, key, nextChildPage, resultPageNum);
}

int32_t IndexManager::getNextChildPage(const Attribute attr, const void *key, void *pageData)
{
    InternalHeader header = getInternalHeader(pageData);
    if (key == NULL)
        return header.leftChildPage;

    int i = 0;
    for (i = 0; i < header.entriesNumber; i++)
    {
        // If key < slot key we have, then the previous entry holds the path
        if (compareSlot(attr, key, pageData, i) <= 0)
            break;
    }
    int32_t result;
    // Special case where key is less than all entries in this node
    if (i == 0)
    {
        result = header.leftChildPage;
    }
    // Usual case where we grab the child to the right of the largest entry less than key
    // This also works if key is larger than all entries
    else
    {
        IndexEntry entry = getIndexEntry(i - 1, pageData);
        result = entry.childPage;
    }
    return result;
}

int IndexManager::compareSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const
{
    IndexEntry entry = getIndexEntry(slotNum, pageData);
    if (attr.type == TypeInt)
    {
        int32_t int_key;
        memcpy(&int_key, key, INT_SIZE);
        return compare(int_key, entry.integer);
    }
    else if (attr.type == TypeReal)
    {
        float real_key;
        memcpy(&real_key, key, REAL_SIZE);
        return compare(real_key, entry.real);
    }
    else
    {
        int32_t key_size;
        memcpy(&key_size, key, VARCHAR_LENGTH_SIZE);
        char key_text[key_size + 1];
        key_text[key_size] = '\0';
        memcpy(key_text, (char*) key + VARCHAR_LENGTH_SIZE, key_size);

        int32_t value_offset = entry.varcharOffset;
        int32_t value_size;
        memcpy(&value_size, (char*)pageData + value_offset, VARCHAR_LENGTH_SIZE);
        char value_text[value_size + 1];
        value_text[value_size] = '\0';
        memcpy(value_text, (char*)pageData + value_offset + VARCHAR_LENGTH_SIZE, value_size);

        return compare(key_text, value_text);
    }
    return 0;
}

int IndexManager::compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const
{
    DataEntry entry = getDataEntry(slotNum, pageData);
    if (attr.type == TypeInt)
    {
        int32_t int_key;
        memcpy(&int_key, key, INT_SIZE);
        return compare(int_key, entry.integer);
    }
    else if (attr.type == TypeReal)
    {
        float real_key;
        memcpy(&real_key, key, REAL_SIZE);
        return compare(real_key, entry.real);
    }
    else
    {
        int32_t key_size;
        memcpy(&key_size, key, VARCHAR_LENGTH_SIZE);
        char key_text[key_size + 1];
        key_text[key_size] = '\0';
        memcpy(key_text, (char*) key + VARCHAR_LENGTH_SIZE, key_size);

        int32_t value_offset = entry.varcharOffset;
        int32_t value_size;
        memcpy(&value_size, (char*)pageData + value_offset, VARCHAR_LENGTH_SIZE);
        char value_text[value_size + 1];
        value_text[value_size] = '\0';
        memcpy(value_text, (char*)pageData + value_offset + VARCHAR_LENGTH_SIZE, value_size);

        return compare(key_text, value_text);
    }
    return 0; // suppress warnings
}

int IndexManager::compare(const int key, const int value) const
{
  // cout << "keyInt: " << key << endl;
  // cout << "valueInt: " << value << endl;
    if (key == value)
        return 0;
    if (key > value)
        return 1;
    if (key < value)
        return -1;
    return 0; // suppress warnings
}

int IndexManager::compare(const float key, const float value) const
{
  // cout << "keyFloat: " << key << endl;
  // cout << "valueFloat: " << value << endl;
    if (key == value)
        return 0;
    if (key > value)
        return 1;
    if (key < value)
        return -1;
    return 0;
}

int IndexManager::compare(const char *key, const char *value) const
{
  // cout << "keyString: " << key << endl;
  // cout << "valueString: " << value << endl;
    return strcmp(key, value);
}

// Get size needed to insert key into page
int IndexManager::getKeyLengthInternal(const Attribute attr, const void *key) const
{
    int size = sizeof(IndexEntry);
    if (attr.type == TypeVarChar)
    {
        int32_t key_len;
        memcpy(&key_len, key, VARCHAR_LENGTH_SIZE);
        size += VARCHAR_LENGTH_SIZE;
        size += key_len;
    }
    return size;
}

int IndexManager::getKeyLengthLeaf(const Attribute attr, const void *key) const
{
    int size = sizeof(DataEntry);
    if (attr.type == TypeVarChar)
    {
        int32_t key_len;
        memcpy(&key_len, key, VARCHAR_LENGTH_SIZE);
        size += VARCHAR_LENGTH_SIZE;
        size += key_len;
    }
    return size;
}

int IndexManager::getFreeSpaceInternal(void *pageData) const
{
    InternalHeader header = getInternalHeader(pageData);
    return header.freeSpaceOffset - (sizeof(NodeType) + sizeof(InternalHeader) + header.entriesNumber * sizeof(IndexEntry));
}

int IndexManager::getFreeSpaceLeaf(void *pageData) const
{
    LeafHeader header = getLeafHeader(pageData);
    return header.freeSpaceOffset - (sizeof(NodeType) + sizeof(LeafHeader) + header.entriesNumber * sizeof(DataEntry));
}

RC IndexManager::deleteEntryFromLeaf(const Attribute attr, const void *key, const RID &rid, void *pageData)
{
    LeafHeader header = getLeafHeader(pageData);

    int i;
    for (i = 0; i < header.entriesNumber; i++)
    {
        // Find a slot whose key and rid are equal to the given key and rid
        if(compareLeafSlot(attr, key, pageData, i) == 0)
        {
            DataEntry entry = getDataEntry(i, pageData);
            if (entry.rid.pageNum == rid.pageNum && entry.rid.slotNum == rid.slotNum)
            {
                break;
            }
        }
    }
    // If we failed to find one, error out
    if (i == header.entriesNumber)
    {
        return IX_RECORD_DN_EXIST;
    }

    DataEntry entry = getDataEntry(i, pageData);
    // Get position where deleted entry starts
    // Then get position where entries end. Move all entries to the left, overwriting the entry being deleted.
    unsigned slotStartOffset = getOffsetOfLeafSlot(i);
    unsigned slotEndOffset = getOffsetOfLeafSlot(header.entriesNumber);
    memmove((char*)pageData + slotStartOffset, (char*)pageData + slotStartOffset + sizeof(DataEntry), slotEndOffset - slotStartOffset - sizeof(DataEntry));

    header.entriesNumber -= 1;

    // Now, if we're a varchar, we need to move all of the varchars over as well
    if (attr.type == TypeVarChar)
    {
        int32_t varcharOffset = entry.varcharOffset;
        int32_t varchar_len;
        memcpy(&varchar_len, (char*)pageData + varcharOffset, VARCHAR_LENGTH_SIZE);
        int32_t entryLen = varchar_len + VARCHAR_LENGTH_SIZE;

        // Take everything from the start of the free space to the start of the varchar being deleted, and move it over the deleted varchar
        memmove((char*)pageData + header.freeSpaceOffset + entryLen, (char*)pageData + header.freeSpaceOffset, varcharOffset - header.freeSpaceOffset);
        header.freeSpaceOffset += entryLen;
        // Update all of the slots that are moved over
        for (i = 0; i < header.entriesNumber; i++)
        {
            entry = getDataEntry(i, pageData);
            if (entry.varcharOffset < varcharOffset)
                entry.varcharOffset += entryLen;
            setDataEntry(entry, i, pageData);
        }
    }
    setLeafHeader(header, pageData);
    return SUCCESS;
}

RC IndexManager::deleteEntryFromInternal(const Attribute attr, const void *key, void *pageData)
{
    InternalHeader header = getInternalHeader(pageData);

    int i;
    for (i = 0; i < header.entriesNumber; i++)
    {
        // Scan through until we find a matching key
        if(compareSlot(attr, key, pageData, i) == 0)
        {
            break;
        }
    }
    if (i == header.entriesNumber)
    {
        // error out if no match
        return IX_RECORD_DN_EXIST;
    }

    IndexEntry entry = getIndexEntry(i, pageData);

    // Get positions where deleted entry starts and end
    unsigned slotStartOffset = getOffsetOfInternalSlot(i);
    unsigned slotEndOffset = getOffsetOfInternalSlot(header.entriesNumber);

    // Move entries over, overwriting the slot being deleted
    memmove((char*)pageData + slotStartOffset, (char*)pageData + slotStartOffset + sizeof(IndexEntry), slotEndOffset - slotStartOffset - sizeof(IndexEntry));
    // Update entriesNumber
    header.entriesNumber -= 1;

    // Now, if we're a varchar, we need to move all of the varchars over as well
    if (attr.type == TypeVarChar)
    {
        int32_t varcharOffset = entry.varcharOffset;
        int32_t varchar_len;
        memcpy(&varchar_len, (char*)pageData + varcharOffset, VARCHAR_LENGTH_SIZE);
        int32_t entryLen = varchar_len + VARCHAR_LENGTH_SIZE;

        // Take everything from start of free space until the starting offset of the varchar, and move it over where the varchar once was
        memmove((char*)pageData + header.freeSpaceOffset + entryLen, (char*)pageData + header.freeSpaceOffset, varcharOffset - header.freeSpaceOffset);
        header.freeSpaceOffset += entryLen;
        // Update all of the slots that are moved over
        for (i = 0; i < header.entriesNumber; i++)
        {
            entry = getIndexEntry(i, pageData);
            if (entry.varcharOffset < varcharOffset)
                entry.varcharOffset += entryLen;
            setIndexEntry(entry, i, pageData);
        }
    }
    setInternalHeader(header, pageData);
    return SUCCESS;
}
