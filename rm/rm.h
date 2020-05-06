
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

#define TABLE_FILE_EXTENSION ".t"
#define INDEX_FILE_EXTENSION ".i"

#define TABLES_TABLE_NAME           "Tables"
#define TABLES_TABLE_ID             1

// Format for Tables table:
// (table-id:int, table-name:varchar(50), file-name:varchar(50), system:int)
// system will be 1 if the table is a system table, 0 otherwise

#define TABLES_COL_TABLE_ID         "table-id"
#define TABLES_COL_TABLE_NAME       "table-name"
#define TABLES_COL_FILE_NAME        "file-name"
#define TABLES_COL_SYSTEM           "system"
#define TABLES_COL_TABLE_NAME_SIZE  50
#define TABLES_COL_FILE_NAME_SIZE   50

// 1 null byte, 2 integer and 2 varchars
#define TABLES_RECORD_DATA_SIZE 1 + 4 * INT_SIZE + TABLES_COL_TABLE_NAME_SIZE + TABLES_COL_FILE_NAME_SIZE

#define COLUMNS_TABLE_NAME           "Columns"
#define COLUMNS_TABLE_ID             2

#define COLUMNS_COL_TABLE_ID         "table-id"
#define COLUMNS_COL_COLUMN_NAME      "column-name"
#define COLUMNS_COL_COLUMN_TYPE      "column-type"
#define COLUMNS_COL_COLUMN_LENGTH    "column-length"
#define COLUMNS_COL_COLUMN_POSITION  "column-position"
#define COLUMNS_COL_COLUMN_NAME_SIZE 50

// 1 null byte, 4 integer fields and a varchar
#define COLUMNS_RECORD_DATA_SIZE 1 + 5 * INT_SIZE + COLUMNS_COL_COLUMN_NAME_SIZE

#define INDEX_TABLE_NAME             "Index"
#define INDEX_TABLE_ID               3

#define INDEX_COL_TABLE_ID           "table-id"
#define INDEX_COL_INDEX_NAME         "index-name"
#define INDEX_COL_ATTR_NAME          "attr-name"
// #define INDEX_COL_ATTR_TYPE          "attr-type"
// #define INDEX_COL_ATTR_LENGTH        "attr-length"
//#define INDEX_COL_COLUMN_POSITION    "index-position"
#define INDEX_COL_COLUMN_NAME_SIZE 150

// 1 null byte, 4 integer fields and a varchar
#define INDEX_RECORD_DATA_SIZE 1 + 3 * INT_SIZE + INDEX_COL_COLUMN_NAME_SIZE + COLUMNS_COL_COLUMN_NAME_SIZE

# define RM_EOF (-1)  // end of a scan operator

#define RM_CANNOT_MOD_SYS_TBL 1
#define RM_NULL_COLUMN        2

typedef struct IndexedAttr
{
    int32_t pos;
    Attribute attr;
} IndexedAttr;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

  friend class RelationManager;
private:
  RBFM_ScanIterator rbfm_iter;
  FileHandle fileHandle;
};

class RM_IndexScanIterator {
public:
   RM_IndexScanIterator(){}; // Constructor
  ~RM_IndexScanIterator(){}; // Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key); // Get next matching entry
  RC close(); // Terminate index scan

  friend class RelationManager;
  friend class IndexManager;
  friend class IXFileHandle;
  friend class IX_ScanIterator;
private:
  IX_ScanIterator ix_iter;
  //IXFileHandle *ixfileHandle;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  bool containsAttribute(const string& attributeName, const vector<Attribute> &attrs, unsigned &retVecIndex);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC indexScan(const string &tableName,
	  const string &attributeName,
	  const void *lowKey,
	  const void *highKey,
	  bool lowKeyInclusive,
	  bool highKeyInclusive,
	  RM_IndexScanIterator &rm_IndexScanIterator);

  static string getIndexFileName(const char *tableName, const string &attrName);
  static string getIndexFileName(const char *tableName, const char* attrName);
  static string getIndexFileName(const string &tableName, const string &attrName);

friend class RM_IndexScanIterator;
friend class RecordBasedFileManager;
friend class IX_ScanIterator;

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  const vector<Attribute> tableDescriptor;
  const vector<Attribute> columnDescriptor;
  const vector<Attribute> indexDescriptor;

  // Convert tableName to file name (append extension)
  static string getFileName(const char *tableName);
  static string getFileName(const string &tableName);
 


  // Create recordDescriptor for Table/Column tables
  static vector<Attribute> createTableDescriptor();
  static vector<Attribute> createColumnDescriptor();
  static vector<Attribute> createIndexDescriptor();

  // Prepare an entry for the Table/Column table
  void prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data);
  void prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data);

  // Given a table ID and recordDescriptor, creates entries in Column table
  RC insertColumns(int32_t id, const vector<Attribute> &recordDescriptor);
  // Given table ID, system flag, and table name, creates entry in Table table
  RC insertTable(int32_t id, int32_t system, const string &tableName);

  // Get next table ID for creating table
  RC getNextTableID(int32_t &table_id);
  // Get table ID of table with name tableName
  RC getTableID(const string &tableName, int32_t &tableID);

  RC isSystemTable(bool &system, const string &tableName);

  RC insertIndex(int32_t id, const string &indexName, const Attribute attribute);

  void prepareIndexRecordData(int32_t id,string indexName, Attribute attr, void *data);

  bool prepareKey(const vector<Attribute> &recordDescriptor, const string &attributeName, const void * data, void * key);
 
  void extractKeyFromRecord(const vector<Attribute> &recordDescriptor, const string &attributeName, const void * data, void * key);



  // Utility functions for converting single values to/from api format
  // Useful when using ScanIterators
  void fromAPI(float &real, void *data);
  void fromAPI(string &str, void *data);
  void fromAPI(int32_t &integer, void *data);
  void toAPI(const float real, void *data);
  void toAPI(const int32_t integer, void *data);
  void toAPI(const string &str, void *data);

};

#endif
