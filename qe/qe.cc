
#include "qe.h"

// ------------------- filter interface -------------------

bool Iterator::compOpCases(int ogComp, int toComp, CompOp op){
    switch(op){
        case EQ_OP: return ogComp == toComp;
        case LT_OP: return ogComp < toComp;
        case GT_OP: return ogComp > toComp;
        case LE_OP: return ogComp <= toComp;
        case GE_OP: return ogComp >= toComp;
        case NE_OP: return ogComp != toComp;
        case NO_OP: return true;
        default: return false;

    }
}

bool Iterator::compOpCases(float ogComp, float toComp, CompOp op){
    switch(op){
        case EQ_OP: return ogComp == toComp;
        case LT_OP: return ogComp < toComp;
        case GT_OP: return ogComp > toComp;
        case LE_OP: return ogComp <= toComp;
        case GE_OP: return ogComp >= toComp;
        case NE_OP: return ogComp != toComp;
        case NO_OP: return true;
        default: return false;
    }
}

bool Iterator::compOpCases(char * ogComp, char * toComp, CompOp op){
	int cmp = strcmp(ogComp, toComp);
    switch (op)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        case NO_OP: return true;
        default: return false;
    }

}

bool Iterator::compValue(void * ogCompPointer, void * toCompPointer, Condition condition)
{
    bool result = false;

    if(condition.op == NO_OP ){
        return true;
    }

    switch(condition.rhsValue.type){
        case TypeInt:
            int32_t ogCompInt;
            int32_t toCompInt;
            memcpy(&ogCompInt, ogCompPointer, INT_SIZE);
            memcpy(&toCompInt, toCompPointer, INT_SIZE);
            // cout << "ogCompInt: " << ogCompInt << endl;
            // cout << "toCompInt: " << toCompInt << endl;
            result = compOpCases(ogCompInt, toCompInt, condition.op);

        break;
        case TypeReal:
            float ogCompFloat;
            float toCompFloat;
            memcpy(&ogCompFloat, ogCompPointer, REAL_SIZE);
            memcpy(&toCompFloat, toCompPointer, REAL_SIZE);
            result = compOpCases(ogCompFloat, toCompFloat, condition.op);

        break;
        case TypeVarChar:
            uint32_t varcharSize = 0;
            memcpy(&varcharSize, ogCompPointer, VARCHAR_LENGTH_SIZE);
            char ogCompChar [varcharSize + 1];
            memcpy(ogCompChar, (char *)ogCompPointer + VARCHAR_LENGTH_SIZE, varcharSize);
            ogCompChar[varcharSize] = '\0';

            memcpy(&varcharSize, toCompPointer, VARCHAR_LENGTH_SIZE);
            char toCompChar [varcharSize + 1];
            memcpy(toCompChar, (char *)toCompPointer + VARCHAR_LENGTH_SIZE, varcharSize);
            toCompChar[varcharSize] = '\0';

            result = compOpCases(ogCompChar, toCompChar, condition.op);

        break;
    }

    return result;

}

bool Iterator::prepAttributeValue(string desiredAttr, vector<Attribute> attrVec, void * data, void * retValue){
    int nullIndicatorSize =  int(ceil((double) attrVec.size() / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    int indicatorIndex;
    int indicatorMask;
    bool fieldIsNull;

    unsigned data_offset = nullIndicatorSize;

    string delimiter = ".";
    // string parsedAttributeName = condition.lhsAttr.substr(condition.lhsAttr.find(delimiter) + 1, condition.lhsAttr.length());
    // //bool attrFoundinVec = false;
    // cout <<"parsedAttributeName: " <<  parsedAttributeName << endl;
    // cout <<"vectorSize: " <<  attrVec.size() << endl;
    // cout << "desiredAttr: " << desiredAttr <<  endl;
    for (unsigned i = 0; i < (unsigned) attrVec.size(); i++)
    {
        // cout << "attName: " << attrVec[i].name << endl;
        indicatorIndex = i / CHAR_BIT;
        indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        fieldIsNull = (nullIndicator[indicatorIndex] & indicatorMask) != 0;
        if (fieldIsNull == true){
            // cout << "Field Is Null" << endl;
            if(attrVec[i].name.compare(desiredAttr) == 0){
                // cout << "Desired Field is Null" << endl;
                return false;
            }
            continue;
        }
        switch (attrVec[i].type)
        {
            case TypeInt:
                if(attrVec[i].name.compare(desiredAttr) == 0){
                	int temp;
                	memcpy(&temp, (char *)data + data_offset, INT_SIZE );
                	// cout << "QEtemp: " << temp << endl;
                    memcpy(retValue,(char *)data + data_offset, INT_SIZE);
                    return true;
                }
                data_offset += INT_SIZE;
            break;
            case TypeReal:
                if(attrVec[i].name.compare(desiredAttr) == 0){
                	float temp2;
                	memcpy(&temp2, (char *)data + data_offset, REAL_SIZE );
                    // cout << "QEtemp2: " << temp2 << endl;
                    memcpy(retValue,(char *)data + data_offset, REAL_SIZE);
                    return true;
                }
                data_offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                if(attrVec[i].name.compare(desiredAttr) == 0){
                    memcpy(&varcharSize, (char*) data + data_offset, VARCHAR_LENGTH_SIZE);
                    char temp3[varcharSize + 1];
                    memcpy(retValue,(char *)data + data_offset, VARCHAR_LENGTH_SIZE);
                    memcpy((char *)retValue + VARCHAR_LENGTH_SIZE,(char *)data + data_offset + VARCHAR_LENGTH_SIZE, varcharSize);
                    memcpy(temp3,(char *)data + data_offset +VARCHAR_LENGTH_SIZE, VARCHAR_LENGTH_SIZE);
                    temp3[varcharSize] = '\0';
                	// cout << "QEtemp3: " << temp3 << endl;
                    return true;
                }
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                data_offset += varcharSize;
                data_offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    // cout << "how did you make it our here?" << endl;
    return false;

}

RC Iterator::prepTupleWAttrVec(vector<Attribute> attrVec, vector<string> desiredAttrs, void * data, void * retTuple){
	void * retValue = malloc(PAGE_SIZE);

    // Get null indicator
    int nullIndicatorSize = int(ceil((double) desiredAttrs.size() / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy (nullIndicator, data, nullIndicatorSize);

    unsigned dataOffset = nullIndicatorSize;
	for (unsigned i = 0; i < desiredAttrs.size(); i++)
	{
	    // Get index and type of attribute in record
	    auto pred = [&](Attribute a) {return a.name == desiredAttrs[i];};
	    auto iterPos = find_if(attrVec.begin(), attrVec.end(), pred);
	    unsigned index = distance(attrVec.begin(), iterPos);
	    if (index == attrVec.size()){
	    	// cout << "No such attribute" << endl;
	    	free(retValue);
	        return RBFM_NO_SUCH_ATTR;
	    }

	    AttrType type = attrVec[index].type;

	    // Read attribute into buffer
	    memset(retValue, 0 ,PAGE_SIZE);

	    // Determine if null
	    if (prepAttributeValue(desiredAttrs[i], attrVec, data, retValue) == false)
	    {
	        int indicatorIndex = i / CHAR_BIT;
	        char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
	        nullIndicator[indicatorIndex] |= indicatorMask;
	    }
	    // Read from buffer into data
	    else if (type == TypeInt)
	    {
	        memcpy ((char*)retTuple + dataOffset, retValue, INT_SIZE);
	        dataOffset += INT_SIZE;
	    }
	    else if (type == TypeReal)
	    {
	        memcpy ((char*)retTuple + dataOffset, retValue, REAL_SIZE);
	        dataOffset += REAL_SIZE;
	    }
	    else if (type == TypeVarChar)
	    {
	        uint32_t varcharSize = 0;
	        memcpy(&varcharSize, retValue, VARCHAR_LENGTH_SIZE);
	        memcpy((char*)retTuple + dataOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
	        dataOffset += VARCHAR_LENGTH_SIZE;
	        memcpy((char*)retTuple + dataOffset, (char *)retValue + VARCHAR_LENGTH_SIZE, varcharSize);
	        dataOffset += varcharSize;
	    }

	}
	memcpy(retTuple, nullIndicator, nullIndicatorSize);
	free(retValue);
	return SUCCESS;

}



unsigned Iterator::getTupleSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = int(ceil((double) recordDescriptor.size() / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = 0;


    int indicatorIndex;
    int indicatorMask;
    bool fieldIsNull;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {

    	indicatorIndex = i / CHAR_BIT;
    	indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    	fieldIsNull = (nullIndicator[indicatorIndex] & indicatorMask) != 0;
    	if (fieldIsNull == true)
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
                uint32_t varcharSize = 0;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                size += VARCHAR_LENGTH_SIZE;
                offset += varcharSize;
                offset += VARCHAR_LENGTH_SIZE;
            break;
        }
    }
    // cout << "Get next tuple size: " << size << endl;
    return size;
}


void Iterator::combineTuples(vector<Attribute> leftAttrs, vector<Attribute> rightAttrs, void * leftTuple, void * rightTuple, void * combinedTuple){
    // cout << "In Combine Tuples: " << endl;
	void * retValue = malloc(PAGE_SIZE);

    // Get null indicator
    int leftNullIndicatorSize = int(ceil((double) leftAttrs.size() / CHAR_BIT));
    int rightNullIndicatorSize = int(ceil((double) rightAttrs.size() / CHAR_BIT));
    int combinedNullIndicatorSize = int(ceil((double)(leftAttrs.size() + rightAttrs.size()) / CHAR_BIT));

    char leftNullIndicator[leftNullIndicatorSize];
    char rightNullIndicator[rightNullIndicatorSize];
    char combinedNullIndicator[combinedNullIndicatorSize];

    memset(leftNullIndicator, 0, leftNullIndicatorSize);
    memset(rightNullIndicator, 0, rightNullIndicatorSize);
    memset(combinedNullIndicator, 0, combinedNullIndicatorSize);

    memcpy (leftNullIndicator, leftTuple, leftNullIndicatorSize);
    memcpy (rightNullIndicator, rightTuple, rightNullIndicatorSize);

    unsigned combinedTupleOffset = combinedNullIndicatorSize;
    unsigned leftTupleSize = getTupleSize(leftAttrs, leftTuple);
    unsigned rightTupleSize = getTupleSize(rightAttrs, rightTuple);


    // cout << "rightAttrs.size()" << rightAttrs.size() <<  endl;
    // cout << "leftAttrs.size()" << leftAttrs.size() <<  endl;
    for (size_t i = 0; i < rightAttrs.size() + leftAttrs.size(); i++)
    {
        // Determine if null
        bool notNull;
        memset(retValue, 0 , PAGE_SIZE);
        if(i < leftAttrs.size()){
            notNull = prepAttributeValue(leftAttrs[i].name, leftAttrs, leftTuple, retValue );
        }else{
            unsigned
            notNull = prepAttributeValue(rightAttrs[i - leftAttrs.size()].name, rightAttrs, rightTuple, retValue );

        }
        if (!notNull)
        {
            int indicatorIndex = i / CHAR_BIT;
            char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
            combinedNullIndicator[indicatorIndex] |= indicatorMask;
        }
    }

    memcpy((char *)combinedTuple + combinedNullIndicatorSize, (char *)leftTuple + leftNullIndicatorSize, leftTupleSize);
    //combinedTupleOffset += leftTupleSize;
    memcpy((char *)combinedTuple + combinedNullIndicatorSize + leftTupleSize , (char *)rightTuple + rightNullIndicatorSize, rightTupleSize);
    //combinedTupleOffset += rightTupleSize;
    memcpy(combinedTuple, combinedNullIndicator, combinedNullIndicatorSize);
    free(retValue);
}


// bool Filter::compValue(void * buffer, Value rhsValue, CompOp op){
//     int ogCompInt = 0;
//     int toCompInt = 0;
//     float ogCompFloat = 0;
//     float toCompFloat = 0;
//     switch(rhsValue.type){
//         case TypeInt:
//             memcpy(&ogCompInt, rhsValue.data, sizeof(int));
//             memcpy(&toCompInt, buffer, sizeof(int));
//             return compOpCases(ogCompInt, toCompInt, op);
//         case TypeReal:
//             memcpy(&ogCompFloat, rhsValue.data, sizeof(float));
//             memcpy(&toCompFloat, buffer, sizeof(float));
//             return compOpCases(ogCompFloat, toCompFloat, op);
//         case TypeVarChar:
//         // deal with
//         // would our value pointer hold the size of the varchar as well?
//         // im guessing yes
//             return false;

//         default: return false;
//     }
// }

Filter::Filter(Iterator* input, const Condition &condition)
{
    // This filter class is initialized by an input iterator and a selection
    // condition. It filters the tuples from the input iterator by applying the
    // filter predicate condition on them. For simplicity, we assume this filter
    // only has a single selection condition. The schema of the returned tuples
    // should be the same as the input tuples from the iterator.

    this->iter = input;
    this->condition = condition;
    this->attrs.clear();
    input->getAttributes(this->attrs);

    //void *data = malloc(PAGE_SIZE);

    // do we call get next Tuple from here
}

RC Filter::getNextTuple(void *data)
{
	RC rc;
    //RC rc = iter->getNextTuple(data);
    void * retValue = malloc(PAGE_SIZE);
    //RC nullValueRC = SUCCESS;

    while(true){
    	memset(retValue, 0 ,PAGE_SIZE);
        rc = iter->getNextTuple(data);

        if(rc == QE_EOF){
	    	free(retValue);
	        return QE_EOF;
        }
        else if(condition.op == NO_OP){
            return SUCCESS;
        }
        else if (prepAttributeValue(condition.lhsAttr, attrs, data, retValue ) == true){
        	if(compValue(retValue, condition.rhsValue.data , condition)){
		    	free(retValue);
	        	return SUCCESS;

        	}
        }

    }
    // bool nullBit = false;
    // bool satisfied = false;
    // RC rc;
    // int offset;
    // void * buffer = malloc(PAGE_SIZE);
    // int compInt = 0;
    // float compFloat = 0;
    // char* compChar = "";


   //load in value into void pointer
//    getCompValue(buffer, condition.rhsValue);

    // recur through the tuples
    // while ((rc = getNextTuple(data)) == 1 )
    // {
    // //   our code
    //   offset = 1;
    //   satisfied = false;



    // // for all attributes in tuple
    //   for(int i =0; attrs.size(); i++){
    //     nullBit = *(unsigned char *)((char *)data) & (1 << (7-i));
    //     // what to do for nullbit case


    //     if(condition.bRhsIsAttr){
    //     //   if (switchCases(condition.op, condition.rhsValue, attrs.)){  //(attrs[i].name == condition.rhsAttr){
    //     //       satisfied = true;
    //     //     // attribute that we care about and want to print
    //     //   }
    //     }
    //     else //bRhsISAttr is False
    //       if(attrs[i].name == condition.lhsAttr){
    //         if(attrs[i].type == TypeVarChar){
    //           // need to load in 4 byte variable first
    //         }else if(attrs[i].type == TypeReal){
    //           //buffer now holds value for corresponding attr
    //           memcpy(buffer, (char*)data+offset, attrs[i].length);
    //         }else{
    //           int compInt;
    //           memcpy(buffer, (char*)data+offset, attrs[i].length);
    //         }

    //         // check to see if rhsValue.data matches comparison condition and value
    //         satisfied = compValue(buffer, condition.rhsValue, condition.op);
    //     }
    //     if(attrs[i].type == TypeVarChar){
    //       offset += 4;
    //     //   offset += sizeofVarChar
    //     }else{
    //       offset += attrs[i].length;
    //     }


    //     // even if isn't attribute we want to comp still memcpy into buffer so we can "keep original tuple schema"



    // }
    //   if(satisfied){ // if tuple satisfied criteria return it
    //     // how do we return a bunch of tuples?
    //     // should we just be printing each one individually?
    //   }
    // }

    // free(buffer);
    // return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = this->attrs;
}


//------------------- project interface -------------------

Project::Project(Iterator *input, const vector<string> &attrNames)
{
    // This project class takes an iterator and a vector of attribute names as input.
    // It projects out the values of the attributes in the attrNames. The schema of
    // the returned tuples should be the attributes in attrNames, in the order of attributes in the vector.

    this->iter = input;
    this->attrNames = attrNames;
    this->attrs.clear();
    input->getAttributes(this->attrs);

}

RC Project::getNextTuple(void *data)
{
	RC rc;
	void * originalTuple = malloc(PAGE_SIZE);

	//Grab the full tuple
    rc = iter->getNextTuple(originalTuple);

    //If end of file return
    if(rc == QE_EOF){
    	free(originalTuple);
        return QE_EOF;
    }
    else{
    	//Else parse through the original tuple
    	prepTupleWAttrVec(attrs, attrNames, originalTuple, data);
    	free(originalTuple);
    	return SUCCESS;
    }
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for(unsigned i = 0; i < this->attrs.size(); i++){
		for(unsigned j = 0; j< this->attrNames.size(); j++){
			if(this->attrs[i].name.compare(this->attrNames[j]) == 0){
				attrs.push_back(this->attrs[i]);
			}
		}

	}

}


// ------------------- index nested loop interface -------------------

INLJoin::INLJoin(
        Iterator *leftIn,           // Iterator of input R
        IndexScan *rightIn,          // IndexScan Iterator of input S
        const Condition &condition   // Join condition
        )
{
        // The INLJoin iterator takes two iterators as input. The leftIn iterator
        // works as the outer relation, and the rightIn iterator is the inner relation.
        // The rightIn is an object of IndexScan Iterator. Again, we have already implemented
        //the IndexScan class for you, which is a wrapper on RM_IndexScanIterator. The
        // returned schema should be the attributes of tuples from leftIn concatenated with the
        // attributes of tuples from rightIn. You don't need to remove any duplicate attributes.
    	this->leftIter = leftIn;
    	this->rightIter = rightIn;

    	this->leftAttrs.clear();
    	this->leftIter->getAttributes(this->leftAttrs);

    	this->rightAttrs.clear();
    	this->rightIter->getAttributes(this->rightAttrs);

        this ->leftConditionValue = malloc(PAGE_SIZE);
        this->rightConditionValue = malloc(PAGE_SIZE);

    	this->condition = condition;
    	this->hitInLeft = false;
    	this->hitInRight = false;
    	this->firstNE_OPRunDone = false;



}

void INLJoin::updateIndexScanIter(Condition condition, void * lhsValue)
{

//          string delimiter = ".";
	// string parsedAttributeName = condition.lhsAttr.substr(condition.lhsAttr.find(delimiter) + 1, condition.lhsAttr.length());

    switch(condition.op){
        case EQ_OP:
         // cout << "In eq op" << endl;
         rightIter->setIterator(lhsValue, lhsValue, true, true);
        break;
        case LT_OP:
         // cout << "In lt op" << endl;
        // Give me all the values of the index where 'S' > 'R'
        rightIter->setIterator(NULL, lhsValue, true, false);
	    // rm.indexScan(tableName, attrName, NULL, condition.rhsValue.data, true,
	    //                false, *iter);
        break;
        case GT_OP:
         // cout << "In lt op" << endl;
        //Give me all the values where 'S' < 'R'
        rightIter->setIterator(lhsValue, NULL, false, true);
        // rm.indexScan(tableName, attrName, condition.rhsValue.data, NULL, false,
        //            true, *iter);
        break;
        case LE_OP:
        // Give me all the values of the index where 'S' >= 'R'
        rightIter->setIterator(NULL, lhsValue, true, true);
        // rm.indexScan(tableName, attrName, NULL, condition.rhsValue.data, true,
        //            true, *iter);

        break;
        case GE_OP:

        // Give me all the values of the index where 'S' <= 'R'
        rightIter->setIterator(lhsValue, NULL , true, true);
        // rm.indexScan(tableName, attrName, condition.rhsValue.data, NULL, true,
        //           true, *iter);

        break;
        case NE_OP:
        // Can run two index scans where 'R' < 'S' and 'R'> 'S'
        // Taking care of the first case!
        rightIter->setIterator(NULL, lhsValue, true, false);
        //rm.indexScan(tableName, attrName, NULL, condition.rhsValue.data, true,
        //           false, *iter);

        break;
        case NO_OP:
        rightIter->setIterator(NULL, NULL, true, true);
        // rm.indexScan(tableName, attrName, NULL, NULL, true,
        //             true, *iter);

        break;
    }


   // rm.indexScan(tableName, parsedAttributeName, lowKey, highKey, lowKeyInclusive,
   //                highKeyInclusive, *iter);
};


//For each tuple r in R:
//	For each tuple s in S:
//     if r and s satisfy the join condition
//			output the tuple <r,s>
RC INLJoin::getNextTuple(void *data)
{
	void * leftTuple = malloc(PAGE_SIZE);
	void * rightTuple = malloc(PAGE_SIZE);
	// void * leftConditionValue = malloc(PAGE_SIZE);
	// void * rightConditionValue = malloc(PAGE_SIZE);
    bool leftValueNotNull;

	RC rc;

	// cout << "Beginning of getNextTuple! " << endl;


	while(true){

        memset(leftTuple, 0, PAGE_SIZE);
        //For each r in R as long as not null? Bc no
        // NUll no-op case??
        while(!hitInLeft){
		memset(leftConditionValue, 0 , PAGE_SIZE);
            // cout << "No left tuple found yet!" << endl;
		    rc = leftIter->getNextTuple(leftTuple);
		    if(rc == QE_EOF){
		    	// cout << "Left tuple EOF" << endl;
		    	free(leftTuple);
		    	free(rightTuple);
		    	// free(leftConditionValue);
		    	// free(rightConditionValue);
		    	return QE_EOF;
		    }
		    // If the rc is not QE_EOF then check if either the condition operator is NO_OP OR if there is an actual value we can compare against
			if(condition.op == NO_OP){
				// cout << "NO_OP" <<  endl;
				hitInLeft = true;
			}
			else if((leftValueNotNull = prepAttributeValue(condition.lhsAttr, leftAttrs, leftTuple, leftConditionValue )) == true ){
				// cout << "Left tuple value found" <<  endl;
                //That means value is not null
                hitInRight = false;
		    	hitInLeft = true;
			}
		}
		//Set Up qualifying S
		if(!hitInRight){
			if(firstNE_OPRunDone == false){
				// cout << "No right tuple found yet! First NE_OP run!" <<endl;
				if(leftValueNotNull || condition.op == NO_OP){
                    updateIndexScanIter(condition,  leftConditionValue);
                    hitInLeft = false;
    				if(condition.op == NE_OP){
    					// cout << "First NE_OP Run" << endl;
    					firstNE_OPRunDone = true;
    				}
                }
			}else{
				// cout << "Second NE_OP Run" << endl;
				rightIter->setIterator(leftConditionValue, NULL, false, true);
				firstNE_OPRunDone = false;
			}
		}
		while(true){
			memset(rightTuple, 0, PAGE_SIZE);
			rc = rightIter->getNextTuple(rightTuple);
			if(rc == QE_EOF){
				//If here we have exhausted S for current r
				// and break the loop to update r all over again
				// cout << "Right Tuple EOF" << endl;
                if(firstNE_OPRunDone){
                    hitInLeft = true;
                    hitInRight = false;

                }else{
    				hitInLeft = false;
                    hitInRight = false;

                }
                break;
            }else{
    			//memset(rightConditionValue, 0, PAGE_SIZE);
                // cout << "Right tuple  NO_OP" << endl;
                combineTuples(leftAttrs, rightAttrs, leftTuple, rightTuple, data);
                hitInRight = true;

                free(leftTuple);
                free(rightTuple);

                return SUCCESS;

            }
			// If value returned good or NO_OP!
			// if((condition.op == NO_OP)){
			// 	cout << "Right tuple  NO_OP" << endl;
		 //    	combineTuples(leftAttrs, rightAttrs, leftTuple, rightTuple, data);
		 //    	hitInRight = true;

   //              free(leftTuple);
   //              free(rightTuple);
   //              free(leftConditionValue);
   //              free(rightConditionValue);

   //              return SUCCESS;

			// }
            //Should actually never happen with indexes
   //          else if((prepAttributeValue(rightIter->attrName, rightAttrs, rightTuple, rightConditionValue ) == true) ) {
		 //    	cout << "Right Tuple found!" << endl;
		 //    	hitInRight = true;
		 //    	//Combine the two tuples and return as data
		 //    	combineTuples(leftAttrs, rightAttrs, leftTuple, rightTuple, data);

		 //    	free(leftTuple);
		 //    	free(rightTuple);
		 //    	free(leftConditionValue);
		 //    	free(rightConditionValue);

		 //    	return SUCCESS;
			// }

		}
	}
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    for(unsigned i = 0; leftAttrs.size(); i++){
    	attrs.push_back(leftAttrs[i]);
    }
    for(unsigned j = 0; rightAttrs.size(); j++){
    	attrs.push_back(rightAttrs[j]);
    }


}
