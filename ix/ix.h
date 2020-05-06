#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

#define IX_TYPE_LEAF     0
#define IX_TYPE_INTERNAL 1

# define IX_EOF (-1)  // end of the index scan
#define IX_CREATE_FAILED          1
#define IX_OPEN_FAILED            2
#define IX_MALLOC_FAILED          3
#define IX_CLOSE_FAILED           4
#define IX_DESTROY_FAILED         5
#define IX_APPEND_FAILED          6
#define IX_READ_FAILED            7
#define IX_RECORD_DN_EXIST        8
#define IX_INSERT_LEAF_FAILED     9
#define IX_BAD_CHILD              10
#define IX_INSERT_INTERNAL_FAILED 11
#define IX_WRITE_FAILED           12
#define IX_NO_FREE_SPACE          13


// Headers and data types

// First byte of each Node gives the type of the node. 0 for leaf, non-zero for internal
typedef char NodeType;

// Leaf nodes contain pointers to prev and next nodes in linked list of leafs
// Also contain number of keys within and pointer to free space
// 0 is always meta node, so a 0 value for next/prev is like NULL
typedef struct LeafHeader
{
	uint32_t next;
	uint32_t prev;
	uint16_t entriesNumber;
	uint16_t freeSpaceOffset;
} LeafHeader;

typedef struct DataEntry
{
	union
	{
		int32_t integer;
		float real;
		int32_t varcharOffset;
	};
	RID rid;
} DataEntry;

// each entry has offset to key and link to child
typedef struct IndexEntry
{
	union
    {
        int32_t integer;
        float real;
        int32_t varcharOffset;
    };
	uint32_t childPage;
} IndexEntry;

// Internal nodes contain number of keys and pointer to free space
typedef struct InternalHeader
{
	uint16_t entriesNumber;
	uint16_t freeSpaceOffset;
    uint32_t leftChildPage;
} InternalHeader;

// Used in insert to carry up result of each recursive insert
typedef struct ChildEntry
{
    void *key;
    uint32_t childPage;
} ChildEntry;

// Header for metadata page, page 0
// Contains pointer to root node so that root node can be moved when split
typedef struct MetaHeader
{
	uint32_t rootPage;
} MetaHeader;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        friend class IX_ScanIterator;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;

        // Utility function for insertEntry
        RC insert(const Attribute &attribute, const void *key, const RID &rid, IXFileHandle &fileHandle, int32_t pageID, ChildEntry &childEntry);
        // Inserts ChildEntry <key, pageNum> into internal node. Returns an error if there's not enough space
        RC insertIntoInternal(const Attribute attribute, ChildEntry entry, void *pageData);
        // Inserts <key, rid> into the given leaf node. Returns an error if there's not enough free space
        RC insertIntoLeaf(const Attribute attribute, const void *key, const RID &rid, void *pageData);

        // Gets offset to a leaf slot with the given slot number
        int getOffsetOfLeafSlot(int slotNum) const;
        // Gets offset to an internal slot with the given slot number
        int getOffsetOfInternalSlot(int slotNum) const;

        // Handles splitting a leaf
        RC splitLeaf(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, const RID rid, const int32_t pageID, void *originalLeaf, ChildEntry &childEntry);
        // Handles splitting an internal node, including the case where the root needs to be split
        RC splitInternal(IXFileHandle &fileHandle, const Attribute &attribute, const int32_t pageID, void *original, ChildEntry &childEntry);

        // Helper functions for printBtree
        void printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const;
        void printInternalNode(IXFileHandle &, void *pageData, const Attribute &attr, string prefix) const;
        void printInternalSlot(const Attribute &attr, const int32_t slotNum, const void *data) const;
        void printLeafNode(void *pageData, const Attribute &attr) const;

        // Each method in this block gets or sets some header data for different types of pages
        void setMetaData(const MetaHeader header, void *pageData);
        MetaHeader getMetaData(const void *pageData) const;
        void setNodeType(const NodeType type, void *pageData);
        NodeType getNodetype(const void *pageData) const;
        void setInternalHeader(const InternalHeader header, void *pageData);
        InternalHeader getInternalHeader(const void *pageData) const;
        void setLeafHeader(const LeafHeader header, void *pageData);
        LeafHeader getLeafHeader(const void *pageData) const;
        void setIndexEntry(const IndexEntry entry, const int slotNum, void *pageData);
        IndexEntry getIndexEntry(const int slotNum, const void *pageData) const;
        void setDataEntry(const DataEntry entry, const int slotNum, void *pageData);
        DataEntry getDataEntry(const int slotNum, const void *pageData) const;

        RC getRootPageNum(IXFileHandle &fileHandle, int32_t &result) const;

        // Finds the leaf page that would contain key
        RC find(IXFileHandle &handle, const Attribute attr, const void *key, int32_t &resultPageNum);
        // Finds the leaf page that would contain key, starting at currPageNum. Utility function for find.
        RC treeSearch(IXFileHandle &handle, const Attribute attr, const void *key, const int32_t currPageNum, int32_t &resultPageNum);
        // Given an attribute, key, and internal node, returns the pagenumber of the childPage who would contain key
        int32_t getNextChildPage(const Attribute attr, const void *key, void *pageData);

        // Compares key to the value in pageDat at slotNum. For internal nodes.
        int compareSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const;
        // Compares key to the value in pageData at slotNum. For leaf nodes.
        int compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const;
        // Returns -1, 0, or 1 if key is less than, equal to, or greater than value
        int compare(const int key, const int value) const;
        int compare(const float key, const float value) const;
        int compare(const char *key, const char *value) const;

        // Returns the amount of space requried to store this key in an internal node
        int getKeyLengthInternal(const Attribute attr, const void *key) const;
        // Returns the amount of space required to store this key in a leaf
        int getKeyLengthLeaf(const Attribute attr, const void *key) const;
        // Returns the amount of free space in the internal node
        int getFreeSpaceInternal(void *pageData) const;
        // Returns the amount of free space in the leaf
        int getFreeSpaceLeaf(void *pageData) const;

        // Deletes an entry with key key and rid rid from leaf given by pageData
        RC deleteEntryFromLeaf(const Attribute attr, const void *key, const RID &rid, void *pageData);
        // Deletes key key from the Internal node given by pageData
        RC deleteEntryFromInternal(const Attribute attr, const void *key, void *pageData);
};

class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;


    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    unsigned getNumberOfPages();

	// Added these
	RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);

    friend class IndexManager;
	friend class RelationManager;
    friend class RM_IndexScanIterator;
	
	private:
        FileHandle fh;

	};

class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        friend class IndexManager;
        friend class RelationManager;
        friend class RM_IndexScanIterator;
    private:
        IXFileHandle *fileHandle;
        Attribute attr;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;


        void *page;
        int slotNum;

        RC initialize(IXFileHandle &, Attribute, const void*, const void*, bool, bool);
};

#endif
