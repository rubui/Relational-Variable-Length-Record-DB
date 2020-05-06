
#include "rm.h"

#include <algorithm>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()),
indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(INDEX_TABLE_NAME));
    if (rc)
        return rc;

    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(INDEX_TABLE_ID, 1, INDEX_TABLE_NAME);
    if (rc)
        return rc;


    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(INDEX_TABLE_ID, indexDescriptor);
    if (rc)
        return rc;

    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(INDEX_TABLE_NAME));
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    IndexManager *ix = IndexManager::instance();
    RC rc;

    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;


    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;
    void * indexNameP = malloc(PAGE_SIZE);
    FileHandle fileHandle;
    RID rid;
    //NEED TO ADD DELETION OF ALL INDEXES ASSOCIATED WITH THE TABLE
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab the indexFileName for each indexFile which shares the same table ID's
    // Delete this from the index table
    // Delete indexFile matching with name
    projection.push_back(INDEX_COL_INDEX_NAME);
    rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    while((rc = rbfm_si.getNextRecord(rid, indexNameP)) == SUCCESS){
        unsigned offset = 1;
        int32_t varcharSize;
        memcpy(&varcharSize, (char*) indexNameP + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char indexName[varcharSize + 1];

        memcpy(indexName, (char*) indexNameP +offset, varcharSize);
        indexName[varcharSize] = '\0';

        rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
        if(rc){
            free(indexNameP);
            return rc;
        }

        rc = ix->destroyFile(indexName);
        if(rc){
            free(indexNameP);
            return rc;
        }
    }

    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    free(indexNameP);
    projection.clear();

    // Open tables file
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF){
        return rc;
    }

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second)
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }
    return SUCCESS;
}


RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    IndexManager *ix = IndexManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);

    rbfm->closeFile(fileHandle);

    //CYCLE THROUGH INDEXES IF THEY EXIST FOR A TABLE FOR INSERTION

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open the Index Table
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    void *value = &id;

   // cout << "beginning of the scan" << endl;

    rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID retRid;
    void * nameBuffer = malloc(PAGE_SIZE);
    char * attributeName = (char *)malloc(COLUMNS_COL_COLUMN_NAME_SIZE);
    char * indexFileName = (char *)malloc(INDEX_COL_COLUMN_NAME_SIZE);
    void * key = malloc(PAGE_SIZE);
    IXFileHandle ixfh;

    //Find all Index Files which matcht the Table ID
    while((rc = rbfm_si.getNextRecord(retRid, NULL)) == SUCCESS)
    {
    	//cout << "In the scan loop" << endl;
       // Prepare all of the pointer
       memset(nameBuffer, 0, PAGE_SIZE);
       memset(attributeName, 0, COLUMNS_COL_COLUMN_NAME_SIZE);
       memset(indexFileName, 0, INDEX_COL_COLUMN_NAME_SIZE);
       uint32_t varcharSize = 0;

       // Extract the Atrtibute information for the Indexed File
       rbfm->readAttribute(fileHandle, indexDescriptor, retRid, INDEX_COL_ATTR_NAME , nameBuffer);
       //Returns back the null indicator, so ignore
       memcpy(&varcharSize, (char *)nameBuffer + 1, VARCHAR_LENGTH_SIZE);
       //cout << "VarCharSize: " << varcharSize << endl;
       memcpy(attributeName, (char *)nameBuffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
       //cout << "attributeName: " << string(attributeName) << endl;

       // Extract the index File name
       memset(nameBuffer, 0, PAGE_SIZE);
       rbfm->readAttribute(fileHandle, indexDescriptor, retRid, INDEX_COL_INDEX_NAME , nameBuffer);
       memcpy(&varcharSize, (char *)nameBuffer + 1, VARCHAR_LENGTH_SIZE);
       //cout << "VarCharSize: " << varcharSize << endl;
       memcpy(indexFileName, (char *)nameBuffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
       //cout << "indexFileName: " << string(indexFileName) << endl;


       //Open the IX file
       rc = ix->openFile(string(indexFileName), ixfh);
       if (rc){
       	   //cout << "Couldn't open the file " << endl;
           return rc;
       }

       memset(key, 0 ,PAGE_SIZE);
       // Prepare the Key for the entry for the given data record
       bool keyBool = prepareKey(recordDescriptor, attributeName, data, key);
       // int wow;
       // memcpy(&wow, key, INT_SIZE);
       // cout << "Wow: " << wow << endl;

       unsigned retVecIndex = 0;

  	   if(containsAttribute(string(attributeName), recordDescriptor, retVecIndex)){
		    if(keyBool){
                rc = ix->insertEntry(ixfh, recordDescriptor[retVecIndex], key, rid);
            }
		    if (rc){
		    	//cout << "InsertEntry Failed!?!?!" << endl;
		    	free(nameBuffer);
		    	free(attributeName);
		    	free(indexFileName);
                free(key);
                return rc;
            }

       }else{
            //cout << "Not Contain attribute?!?!" << endl;
            free(key);
	    	free(nameBuffer);
	    	free(attributeName);
	    	free(indexFileName);
  	   		return -1;
  	   }

  	   ix->closeFile(ixfh);

    }

	free(nameBuffer);
	free(attributeName);
	free(indexFileName);
    free(key);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;

}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    IndexManager *ix = IndexManager::instance();
    RC rc;
    FileHandle tableFileHandle;
    FileHandle indexTableFileHandle;
    RID retRid;
    void * nameBuffer = malloc(PAGE_SIZE);
    char * attributeName = (char *)malloc(COLUMNS_COL_COLUMN_NAME_SIZE);
    char * indexFileName = (char *)malloc(INDEX_COL_COLUMN_NAME_SIZE);
    void * key = malloc(PAGE_SIZE);
    void * retData = malloc(PAGE_SIZE);
    IXFileHandle ixfh;


    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc){
        free(retData);
        free(key);
        free(nameBuffer);
        free(attributeName);
        free(indexFileName);
        return rc;
    }
    if (isSystem){
        free(retData);
        free(key);
        free(nameBuffer);
        free(attributeName);
        free(indexFileName);
        return RM_CANNOT_MOD_SYS_TBL;
    }

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc){
        free(retData);
        free(key);
        free(nameBuffer);
        free(attributeName);
        free(indexFileName);
        return rc;
    }

    // And get fileHandle
    rc = rbfm->openFile(getFileName(tableName), tableFileHandle);
    if (rc){
        free(retData);
        free(key);
        free(nameBuffer);
        free(attributeName);
        free(indexFileName);
        return rc;
    }


    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc){
        free(retData);
        free(key);
        free(nameBuffer);
        free(attributeName);
        free(indexFileName);
        return rc;
    }

    // Open the Index Table
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), indexTableFileHandle);
    if (rc){
        free(retData);
        free(key);
        free(nameBuffer);
        free(attributeName);
        free(indexFileName);
        return rc;
    }

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    void *value = &id;


    rc = rbfm->scan(indexTableFileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    //Find all Index Files which matcht the Table ID
    while((rc = rbfm_si.getNextRecord(retRid, NULL)) == SUCCESS)
    {
       // Prepare all of the pointer
       memset(nameBuffer, 0, PAGE_SIZE);
       memset(retData, 0, PAGE_SIZE);
       memset(key, 0, PAGE_SIZE);
       memset(attributeName, 0, COLUMNS_COL_COLUMN_NAME_SIZE);
       memset(indexFileName, 0, INDEX_COL_COLUMN_NAME_SIZE);
       uint32_t varcharSize = 0;

       // Extract the Atrtibute information for the Indexed File
       rbfm->readAttribute(indexTableFileHandle, indexDescriptor, retRid, INDEX_COL_ATTR_NAME , nameBuffer);
       //Returns back the null indicator, so ignore
       memcpy(&varcharSize, (char *)nameBuffer + 1, VARCHAR_LENGTH_SIZE);
       memcpy(attributeName, (char *)nameBuffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);

       // Extract the index File name
       memset(nameBuffer, 0, PAGE_SIZE);
       rbfm->readAttribute(indexTableFileHandle, indexDescriptor, retRid, INDEX_COL_INDEX_NAME , nameBuffer);
       memcpy(&varcharSize, (char *)nameBuffer + 1, VARCHAR_LENGTH_SIZE);
       memcpy(indexFileName, (char *)nameBuffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);

	   rbfm->readAttribute(tableFileHandle, recordDescriptor, retRid, string(attributeName) , retData);

       //Open the IX file
       rc = ix->openFile(string(indexFileName), ixfh);
       if (rc){
            free(retData);
            free(key);
            free(nameBuffer);
            free(attributeName);
            free(indexFileName);
           return rc;
       }

       memset(key, 0 ,PAGE_SIZE);
       // Prepare the Key for the entry for the given data record
       bool keyBool = prepareKey(recordDescriptor, attributeName, retData, key);

       unsigned retVecIndex = 0;

       if(containsAttribute(string(attributeName), recordDescriptor, retVecIndex)){
            if(keyBool){
                rc = ix->deleteEntry(ixfh, recordDescriptor[retVecIndex], key, rid);

            }
            if (rc){
                free(retData);
                free(key);
                free(nameBuffer);
                free(attributeName);
                free(indexFileName);
                return rc;
            }

       }else{
            //cout << "No attribute?!?!" << endl;
            free(key);
            free(nameBuffer);
            free(attributeName);
            free(indexFileName);
            free(retData);
            return -1;
       }

       ix->closeFile(ixfh);

    }

    free(key);
    free(nameBuffer);
    free(attributeName);
    free(indexFileName);
    free(retData);
    rbfm->closeFile(indexTableFileHandle);
    rbfm->closeFile(tableFileHandle);
    rbfm_si.close();
    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    IndexManager *ix = IndexManager::instance();
    RC rc;
    FileHandle tableFileHandle;
    FileHandle indexTableFileHandle;
    RID retRid;
    void * nameBuffer = malloc(PAGE_SIZE);
    char * attributeName = (char *)malloc(COLUMNS_COL_COLUMN_NAME_SIZE);
    char * indexFileName = (char *)malloc(INDEX_COL_COLUMN_NAME_SIZE);
    void * newKey = malloc(PAGE_SIZE);
    void * oldKey = malloc(PAGE_SIZE);
    void * retData = malloc(PAGE_SIZE);
    IXFileHandle ixfh;


    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    rc = rbfm->openFile(getFileName(tableName), tableFileHandle);
    if (rc)
        return rc;


    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open the Index Table
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), indexTableFileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    void *value = &id;


    rc = rbfm->scan(indexTableFileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    //Find all Index Files which matcht the Table ID
    while((rc = rbfm_si.getNextRecord(retRid, NULL)) == SUCCESS)
    {
       // Prepare all of the pointer
       memset(nameBuffer, 0, PAGE_SIZE);
       memset(retData, 0, PAGE_SIZE);
       memset(newKey, 0, PAGE_SIZE);
       memset(oldKey, 0, PAGE_SIZE);
       memset(attributeName, 0, COLUMNS_COL_COLUMN_NAME_SIZE);
       memset(indexFileName, 0, INDEX_COL_COLUMN_NAME_SIZE);
       uint32_t varcharSize = 0;

       // Extract the Atrtibute information for the Indexed File
       rbfm->readAttribute(indexTableFileHandle, indexDescriptor, retRid, INDEX_COL_ATTR_NAME , nameBuffer);
       //Returns back the null indicator, so ignore
       memcpy(&varcharSize, (char *)nameBuffer + 1, VARCHAR_LENGTH_SIZE);
       memcpy(attributeName, (char *)nameBuffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);

       // Extract the index File name
       memset(nameBuffer, 0, PAGE_SIZE);
       rbfm->readAttribute(indexTableFileHandle, indexDescriptor, retRid, INDEX_COL_INDEX_NAME , nameBuffer);
       memcpy(&varcharSize, (char *)nameBuffer + 1, VARCHAR_LENGTH_SIZE);
       memcpy(indexFileName, (char *)nameBuffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);

	   rbfm->readAttribute(tableFileHandle, recordDescriptor, retRid, string(attributeName) , retData);

       //Open the IX file
       rc = ix->openFile(string(indexFileName), ixfh);
       if (rc)
           return rc;

       // Prepare the Key for the entry for the given data record
       bool oldKeyBool = prepareKey(recordDescriptor, string(attributeName), retData, oldKey);

       bool newKeyBool = prepareKey(recordDescriptor, string(attributeName), data, newKey);
       unsigned retVecIndex = 0;

       if(containsAttribute(string(attributeName), recordDescriptor, retVecIndex)){
            rc = SUCCESS;
            if(oldKeyBool){
                rc = ix->deleteEntry(ixfh, recordDescriptor[retVecIndex], oldKey, rid);
            }
            if(newKeyBool){
                rc = ix->insertEntry(ixfh, recordDescriptor[retVecIndex], newKey, rid);

            }
            if (rc){
                free(nameBuffer);
                free(attributeName);
                free(indexFileName);
                free(newKey);
                free(oldKey);
                free(retData);
                return rc;
            }

       }else{
            free(nameBuffer);
            free(attributeName);
            free(indexFileName);
            free(newKey);
            free(oldKey);
            free(retData);
            return -1;
       }

       ix->closeFile(ixfh);

    }


    // Let rbfm do all the work
    rc = rbfm->updateRecord(tableFileHandle, recordDescriptor, data, rid);

    free(nameBuffer);
    free(attributeName);
    free(indexFileName);
    free(newKey);
    free(oldKey);
    free(retData);
    rbfm->closeFile(tableFileHandle);
    rbfm->closeFile(indexTableFileHandle);
    rbfm_si.close();
    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getIndexFileName(const char *tableName, const string &attrName){

    return string(tableName) + attrName +  string(INDEX_FILE_EXTENSION);
}

string RelationManager::getIndexFileName(const char *tableName, const char *attrName){

    return string(tableName) + string(attrName) +  string(INDEX_FILE_EXTENSION);
}

string RelationManager::getIndexFileName(const string &tableName, const string &attrName){
	// cout << "In two string getIndexFileName " << tableName + attrName + string(INDEX_FILE_EXTENSION) <<endl;
    return tableName + attrName + string(INDEX_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

vector<Attribute> RelationManager::createIndexDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = INDEX_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = INDEX_COL_INDEX_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEX_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = INDEX_COL_ATTR_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    // attr.name = INDEX_COL_ATTR_TYPE;
    // attr.type = TypeInt;
    // attr.length = (AttrLength)INT_SIZE;
    // cd.push_back(attr);

    // attr.name = INDEX_COL_ATTR_LENGTH;
    // attr.type = TypeVarChar;
    // attr.length = (AttrLength)INDEX_COL_COLUMN_NAME_SIZE;
    // cd.push_back(attr);

    return cd;

}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len;
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF){
        rc = SUCCESS;
    }

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &len, INT_SIZE);
    memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*) data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*) data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*) data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*) data + 1, REAL_SIZE);

    real = tmp;
}

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}


RC RelationManager::indexScan(const string &tableName,
	  const string &attributeName,
	  const void *lowKey,
	  const void *highKey,
	  bool lowKeyInclusive,
	  bool highKeyInclusive,
	  RM_IndexScanIterator &rm_IndexScanIterator)
  {

  	//RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  	IndexManager *ix = IndexManager::instance();

   // cout << "Before open!" << endl;
    // Open the file for the given tableName

    //SOMETHING DEFINITELY WRONG WITH ixfh??
    //IXFileHandle ixfh;
    //rm_IndexScanIterator.ix_iter.fileHandle = &ixfh;
    //cout << getIndexFileName(tableName, attributeName) << endl;


    //cout << "After open!" << endl;
    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    RC rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    unsigned retVecIndex = 0;

    if(containsAttribute(attributeName, recordDescriptor, retVecIndex)){

        rm_IndexScanIterator.ix_iter.fileHandle = new IXFileHandle();
        rc = ix->openFile(getIndexFileName(tableName, attributeName), *(rm_IndexScanIterator.ix_iter.fileHandle));
        if (rc){
            cout << "Could not open file" << endl;
            return rc;
        }
        // cout << "containsAttribute" << endl;
        //cout << "It contains the attribute!" << endl;
	  	// Use the underlying ix_scaniterator to do all the work
	  	rc = ix->scan(*(rm_IndexScanIterator.ix_iter.fileHandle),
	                recordDescriptor[retVecIndex],
	                lowKey,
	                highKey,
	                lowKeyInclusive,
	                highKeyInclusive,
	                rm_IndexScanIterator.ix_iter);

        if(rc){
            cout << "Something wring with scan?" << endl;
            return rc;
        }

        return SUCCESS;
	}else{
        cout << "Does not contain attribute" << endl;
		return -1;
	}

  	return SUCCESS;
  	//return ix_ScanIterator.initialize(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);


  }

  // Let ix do all the work
  RC RM_IndexScanIterator::getNextEntry(RID &rid, void *data)
  {
     return ix_iter.getNextEntry(rid, data);
  }

  // Close our file handle, rbfm_scaniterator
  RC RM_IndexScanIterator::close()
  {
      IndexManager *ix = IndexManager::instance();
      RC rc = ix_iter.close();
       if(rc){
        cout << "Could not close ix_iter" <<  endl;
         return rc;
       }
      ix->closeFile(*(ix_iter.fileHandle));
      if(rc){
        cout << "Could not close ix fileHandle" <<  endl;
         return rc;
       }
      delete ix_iter.fileHandle;

      return SUCCESS;
  }
// The Index catalog should contain information about the table that the index is defined on,
// the attribute of that table that the index is defined on, and the Record-Based File in
// which the data corresponding to the index is stored.
  RC RelationManager::createIndex(const string &tableName, const string &attributeName){
      // return error when file already exists
      RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
      IndexManager *ix = IndexManager::instance();
      RC rc;

      vector<Attribute> attrs;
      vector<string> projection; // Contains onlt the desired attribute
      vector<Attribute> recordDescriptorSingle;
      RBFM_ScanIterator rbfm_si;
      IXFileHandle ixfh;
      FileHandle fh;

      bool isSystem;
      rc = isSystemTable(isSystem, tableName);
      if (rc)
         return rc;
      if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;


      // Search the TABLE.t for the table ID
      // Grab the table ID
      int32_t id;
      rc = getTableID(tableName, id);
      if (rc)
          return rc;

      // Get recordDescriptor
      vector<Attribute> recordDescriptor;
      unsigned retVecIndex;
      rc = getAttributes(tableName, recordDescriptor);


      //Use the helper function to isolate the desired attribute
      if(containsAttribute(attributeName, recordDescriptor, retVecIndex)){

        //cout << "Create Index: found attribute" << endl;

        rc = insertIndex(id, getIndexFileName(tableName, attributeName), recordDescriptor[retVecIndex]);
        if (rc)
              return rc;

        rc = ix->createFile(getIndexFileName(tableName, attributeName));
        if (rc)
            return rc;

       // cout << "After indexFileName" << endl;
        projection.clear();
        projection.push_back(attributeName);
        recordDescriptorSingle.push_back(recordDescriptor[retVecIndex]);

  		//Set up a scan that inserts the Entry , Project on only the needed attribute

      	rc = rbfm->openFile(getFileName(tableName), fh);
      	if (rc)
          return rc;

      	rc = ix->openFile(getIndexFileName(tableName, attributeName), ixfh);
      	if (rc)
          return rc;

        rc = rbfm->scan(fh, recordDescriptor, attributeName, NO_OP, NULL, projection, rbfm_si);
        if (rc)
          return rc;

      	void * data = malloc(PAGE_SIZE);
      	void * key = malloc(recordDescriptor[retVecIndex].length + VARCHAR_LENGTH_SIZE);
        memset(data, 0 , PAGE_SIZE);
        memset(key, 0 ,recordDescriptor[retVecIndex].length + VARCHAR_LENGTH_SIZE);
      	RID rid;

      	while((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS){

      	//	cout << "In loop " << endl;
      		if(prepareKey(recordDescriptorSingle, attributeName, data, key) == true){
      		    rc = ix->insertEntry(ixfh, recordDescriptor[retVecIndex], key, rid);
                if (rc)
                    return -1;
            }
      	}

      	//cout << "Create Index  after loops "<< endl;

      	if (rc != RBFM_EOF){
            free(data);
            free(key);
      	    return rc;
        }

      	rbfm_si.close();
        rbfm->closeFile(fh);
        ix->closeFile(ixfh);

      	//cout << "Create Index successful "<< endl;
        free(data);
        free(key);
        return SUCCESS;

      }else{
          return -1;
      }
  }

  RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
      RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
      RC rc;

      // Delete the Index File
      rc = rbfm->destroyFile(getIndexFileName(tableName, attributeName));
      if (rc)
          return rc;

      // Grab the table ID
      int32_t id;
      rc = getTableID(tableName, id);
      if (rc)
          return rc;

      // Open Index tables file
      FileHandle fileHandle;
      rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
      if (rc)
          return rc;

      // Find entry with same table ID
      // Use empty projection because we only care about RID
      RBFM_ScanIterator rbfm_si;
      vector<string> projection; // Empty
      void * value = malloc(INDEX_COL_COLUMN_NAME_SIZE);
      string fullIndexFileName = getIndexFileName(tableName, attributeName);
      uint32_t nameSize = fullIndexFileName.length();

      memcpy(value,&nameSize, VARCHAR_LENGTH_SIZE);
      memcpy((char *)value + VARCHAR_LENGTH_SIZE ,&fullIndexFileName, nameSize);


      rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_INDEX_NAME , EQ_OP, value, projection, rbfm_si);

      RID rid;
      while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
      {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
        if (rc){
            free(value);
            return rc;
        }
      }

      if (rc != RBFM_EOF){
        free(value);
        return rc;
      }

      rbfm->closeFile(fileHandle);
      rbfm_si.close();
      free(value);
      return SUCCESS;
  }

  RC RelationManager::insertIndex(int32_t id, const string &indexName, const Attribute attribute)
  {
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    //cout << "In insertIndex " << endl;
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *indexData = malloc(INDEX_RECORD_DATA_SIZE);
    RID rid;

   // cout << "Before prepareIndexRecordData " << endl;
    prepareIndexRecordData(id, indexName, attribute, indexData);

    //cout << "After prepareIndexRecordData " << endl;
    //cout << indexName << endl;
    rc = rbfm->insertRecord(fileHandle, indexDescriptor, indexData, rid);
    //cout << "After insertRecord" << endl;
    if (rc)
        return rc;

    rbfm->closeFile(fileHandle);
    free(indexData);
   // cout << "return insertIndex " << endl;
    return SUCCESS;


  }

  void RelationManager::prepareIndexRecordData(int32_t id, string indexName, Attribute attr, void *data)
  {


    unsigned offset = 0;
    int32_t name_len = attr.name.length();
    int32_t index_name_len = indexName.length();


  	// All fields non-null
  	char null = 0;
  	// Copy in null indicator
  	memcpy((char*) data + offset, &null, 1);
  	offset += 1;

    //Insert the table ID
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    // Insert the indexFileName
    //cout << "prepareIndexRecordData: indexName " << indexName << endl;
    //cout << "prepareIndexRecordData: indexNameLength " << index_name_len << endl;
    memcpy((char*) data + offset, &index_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, indexName.c_str(), index_name_len);
    offset += index_name_len;

    //Insert the Attribute Name
    //cout << "prepareIndexRecordData: attrName " << attr.name << endl;
    //cout << "prepareIndexRecordData: indexNameLength " << name_len << endl;
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

  // //Insert Attribute Type
  // int32_t type = attr.type;
  // memcpy((char*) data + offset, &type, INT_SIZE);
  // offset += INT_SIZE;

  // //Insert Attribute Lengt
  // int32_t len = attr.length;
  // memcpy((char*) data + offset, &len, INT_SIZE);
  // offset += INT_SIZE;



  }

  // Helper Function to return the key matching a provided attribute
  bool RelationManager::prepareKey(const vector<Attribute> &recordDescriptor, const string &attributeName, const void * data, void * key){
  	    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	    int nullIndicatorSize = rbfm->getNullIndicatorSize(recordDescriptor.size());
	    char nullIndicator[nullIndicatorSize];
	    memset (nullIndicator, 0, nullIndicatorSize);
	    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

	    // Offset into *data
	    unsigned data_offset = nullIndicatorSize;

	    unsigned retVecIndex = 0;

	    Attribute attribute;
		if(containsAttribute(attributeName, recordDescriptor, retVecIndex)){
			attribute = recordDescriptor[retVecIndex];

		}else{
			cout << "how did you get here?" << endl;
            return false;
		}

	    unsigned i = 0;
	    for (i = 0; i < recordDescriptor.size(); i++)
	    {
	        if (!rbfm->fieldIsNull(nullIndicator, i) )
	        {
	            // Points to current position in *data
	            char *data_start = (char*) data + data_offset;

	            // Read in the data for the next column, point rec_offset to end of newly inserted data
	            switch (recordDescriptor[i].type)
	            {
	                case TypeInt:
	                   if (attribute.name.compare(recordDescriptor[i].name) == 0){
		                    memcpy (key, data_start, INT_SIZE);
                            int temp;
                            memcpy (&temp, data_start, REAL_SIZE);
                            // cout << "prepare temp: " << temp << endl;
                            return true;
                       }
                        data_offset += INT_SIZE;
                    break;
                    case TypeReal:
                       if (attribute.name.compare(recordDescriptor[i].name) == 0){
                            memcpy (key, data_start, REAL_SIZE);
                            float temp2;
                            memcpy (&temp2, data_start, REAL_SIZE);
                            // cout << "prepare temp2: " << temp2 << endl;
                            return true;
                       }
                        data_offset += REAL_SIZE;
                    break;
                    case TypeVarChar:
                        uint32_t varcharSize;
                        // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                        memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                        char varChar[varcharSize + 1];
                        //cout << "varcharSize: " << varcharSize << endl;
                        if (attribute.name.compare(recordDescriptor[i].name) == 0){

                            memcpy (key, data_start, VARCHAR_LENGTH_SIZE);
                            memcpy (key, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                            memcpy (varChar, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                            varChar[varcharSize + 1] = '\0';
                            //cout << varChar << endl;
                            return true;


                        }
	                    // We also have to account for the overhead given by that integer.
	                    data_offset += VARCHAR_LENGTH_SIZE;
                        data_offset += varcharSize;
	                break;
	            }
	        }else{
                return false;
            }

	    }
        return false;

  }



  bool RelationManager::containsAttribute(const string& attributeName, const vector<Attribute> &attrs, unsigned &retVecIndex){
	auto it = std::find_if(attrs.begin(), attrs.end(), [&attributeName] (const Attribute& a) { return (attributeName.compare(a.name) == 0); });

	if (it != attrs.end())
	{
		retVecIndex = distance(attrs.begin(), it);
		//cout << "containsAttribute: retVecIndex " << retVecIndex << endl;
		return true;

	}
	else
	{
		retVecIndex = -1;
		return false;
	}
  }
