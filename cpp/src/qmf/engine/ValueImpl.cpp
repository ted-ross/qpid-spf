/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "qmf/engine/ValueImpl.h"
#include <qpid/framing/FieldValue.h>
#include <qpid/framing/FieldTable.h>
#include <qpid/framing/List.h>
#include <qpid/log/Statement.h>

using namespace std;
using namespace qmf::engine;
//using qpid::framing::Buffer;
//using qpid::framing::FieldTable;
//using qpid::framing::FieldValue;
using namespace qpid::framing;

ValueImpl::ValueImpl(Typecode t, Buffer& buf) : typecode(t)
{
    uint64_t first;
    uint64_t second;
    FieldTable ft;
    List fl;

    switch (typecode) {
    case TYPE_UINT8     : value.u32 = (uint32_t) buf.getOctet();  break;
    case TYPE_UINT16    : value.u32 = (uint32_t) buf.getShort();  break;
    case TYPE_UINT32    : value.u32 = (uint32_t) buf.getLong();   break;
    case TYPE_UINT64    : value.u64 = buf.getLongLong();          break;
    case TYPE_SSTR      : buf.getShortString(stringVal);          break;
    case TYPE_LSTR      : buf.getMediumString(stringVal);         break;
    case TYPE_ABSTIME   : value.s64 = buf.getLongLong();          break;
    case TYPE_DELTATIME : value.u64 = buf.getLongLong();          break;
    case TYPE_BOOL      : value.boolVal = (buf.getOctet() != 0);  break;
    case TYPE_FLOAT     : value.floatVal = buf.getFloat();        break;
    case TYPE_DOUBLE    : value.doubleVal = buf.getDouble();      break;
    case TYPE_INT8      : value.s32 = (int32_t) ((int8_t) buf.getOctet()); break;
    case TYPE_INT16     : value.s32 = (int32_t) ((int16_t) buf.getShort()); break;
    case TYPE_INT32     : value.s32 = (int32_t) buf.getLong();    break;
    case TYPE_INT64     : value.s64 = buf.getLongLong();          break;
    case TYPE_UUID      : buf.getBin128(value.uuidVal);           break;
    case TYPE_REF:
        first = buf.getLongLong();
        second = buf.getLongLong();
        refVal.impl->setValue(first, second);
        break;

    case TYPE_MAP:
        ft.decode(buf);
        initMap(ft);
        break;

    case TYPE_LIST:
        fl.decode(buf);
        initList(fl);
        break;

    case TYPE_ARRAY:
    case TYPE_OBJECT:
    default:
        break;
    }
}

ValueImpl::ValueImpl(Typecode t, Typecode at) : typecode(t), valid(false), arrayTypecode(at)
{
}

ValueImpl::ValueImpl(Typecode t) : typecode(t)
{
    ::memset(&value, 0, sizeof(value));
}

Value* ValueImpl::factory(Typecode t, Buffer& b)
{
    ValueImpl* impl(new ValueImpl(t, b));
    return new Value(impl);
}

Value* ValueImpl::factory(Typecode t)
{
    ValueImpl* impl(new ValueImpl(t));
    return new Value(impl);
}

ValueImpl::~ValueImpl()
{
}

void ValueImpl::initMap(const FieldTable& ft)
{
    for (FieldTable::ValueMap::const_iterator iter = ft.begin();
         iter != ft.end(); iter++) {
        const string&     name(iter->first);
        const FieldValue& fvalue(*iter->second);
        uint8_t amqType = fvalue.getType();

        if (amqType == 0x32) {
            Value* subval(new Value(TYPE_UINT64));
            subval->setUint64(fvalue.get<int64_t>());
            insert(name.c_str(), subval);
        } else if ((amqType & 0xCF) == 0x02) {
            Value* subval(new Value(TYPE_UINT32));
            switch (amqType) {
            case 0x02 : subval->setUint(fvalue.get<int>()); break;
            case 0x12 : subval->setUint(fvalue.get<int>()); break;
            case 0x22 : subval->setUint(fvalue.get<int>()); break;
            }
            insert(name.c_str(), subval);
        } else if (amqType == 0x31) {   // int64
            Value* subval(new Value(TYPE_INT64));
            subval->setInt64(fvalue.get<int64_t>());
            insert(name.c_str(), subval);
        } else if ((amqType & 0xCF) == 0x01) {  // 0x01:int8, 0x11:int16, 0x21:int21
            Value* subval(new Value(TYPE_INT32));
            subval->setInt((int32_t)fvalue.get<int>());
            insert(name.c_str(), subval);
        } else if (amqType == 0x85 || amqType == 0x95) {
            Value* subval(new Value(TYPE_LSTR));
            subval->setString(fvalue.get<string>().c_str());
            insert(name.c_str(), subval);
        } else if (amqType == 0x23 || amqType == 0x33) {
            Value* subval(new Value(TYPE_DOUBLE));
            subval->setDouble(fvalue.get<double>());
            insert(name.c_str(), subval);
        } else if (amqType == 0xa8) {
            FieldTable subFt;
            bool valid = qpid::framing::getEncodedValue<FieldTable>(iter->second, subFt);
            if (valid) {
                Value* subval(new Value(TYPE_MAP));
                subval->impl->initMap(subFt);
                insert(name.c_str(), subval);
            }
        } else if (amqType == 0xa9) {
            List subList;
            bool valid = qpid::framing::getEncodedValue<List>(iter->second, subList);
            if (valid) {
                Value* subval(new Value(TYPE_LIST));
                subval->impl->initList(subList);
                insert(name.c_str(), subval);
            }
        } else if (amqType == 0x08) {
            Value* subval(new Value(TYPE_BOOL));
            subval->setBool(fvalue.get<int>() ? true : false);
            insert(name.c_str(), subval);
        } else {
            QPID_LOG(error, "Unable to decode unsupported AMQP typecode=" << amqType << " map index=" << name);
        }
    }
}

void ValueImpl::mapToFieldTable(FieldTable& ft) const
{
    FieldTable subFt;

    for (map<string, Value>::const_iterator iter = mapVal.begin();
         iter != mapVal.end(); iter++) {
        const string& name(iter->first);
        const Value& subval(iter->second);

        switch (subval.getType()) {
        case TYPE_UINT8:
        case TYPE_UINT16:
        case TYPE_UINT32:
            ft.setUInt64(name, (uint64_t) subval.asUint());
            break;
        case TYPE_UINT64:
        case TYPE_DELTATIME:
            ft.setUInt64(name, subval.asUint64());
            break;
        case TYPE_SSTR:
        case TYPE_LSTR:
            ft.setString(name, subval.asString());
            break;
        case TYPE_INT64:
        case TYPE_ABSTIME:
            ft.setInt64(name, subval.asInt64());
            break;
        case TYPE_BOOL:
            ft.set(name, FieldTable::ValuePtr(new BoolValue(subval.asBool())));
            break;
        case TYPE_FLOAT:
            ft.setFloat(name, subval.asFloat());
            break;
        case TYPE_DOUBLE:
            ft.setDouble(name, subval.asDouble());
            break;
        case TYPE_INT8:
        case TYPE_INT16:
        case TYPE_INT32:
            ft.setInt(name, subval.asInt());
            break;
        case TYPE_MAP:
            subFt.clear();
            subval.impl->mapToFieldTable(subFt);
            ft.setTable(name, subFt);
            break;
        case TYPE_LIST:
            {
                List subList;
                subval.impl->listToFramingList(subList);
                ft.set(name,
                       ::qpid::framing::FieldTable::ValuePtr(
                                                             new ListValue(
                                                                           subList)));
            } break;
        case TYPE_ARRAY:
        case TYPE_OBJECT:
        case TYPE_UUID:
        case TYPE_REF:
        default:
            break;
        }
    }
 }


void ValueImpl::initList(const List& fl)
{
    for (List::const_iterator iter = fl.begin();
         iter != fl.end(); iter++) {
        const FieldValue& fvalue(*iter->get());
        uint8_t amqType = fvalue.getType();

        if (amqType == 0x32) {
            Value* subval(new Value(TYPE_UINT64));
            subval->setUint64(fvalue.get<int64_t>());
            appendToList(subval);
        } else if ((amqType & 0xCF) == 0x02) {
            Value* subval(new Value(TYPE_UINT32));
            switch (amqType) {
            case 0x02 : subval->setUint(fvalue.get<int>()); break; // uint8
            case 0x12 : subval->setUint(fvalue.get<int>()); break; // uint16
            case 0x22 : subval->setUint(fvalue.get<int>()); break; // uint32
            }
            appendToList(subval);
        } else if (amqType == 0x31) {   // int64
            Value* subval(new Value(TYPE_INT64));
            subval->setInt64(fvalue.get<int64_t>());
            appendToList(subval);
        } else if ((amqType & 0xCF) == 0x01) {  // 0x01:int8, 0x11:int16, 0x21:int32
            Value* subval(new Value(TYPE_INT32));
            subval->setInt((int32_t)fvalue.get<int>());
            appendToList(subval);
        } else if (amqType == 0x85 || amqType == 0x95) {
            Value* subval(new Value(TYPE_LSTR));
            subval->setString(fvalue.get<string>().c_str());
            appendToList(subval);
        } else if (amqType == 0x23 || amqType == 0x33) {
            Value* subval(new Value(TYPE_DOUBLE));
            subval->setDouble(fvalue.get<double>());
            appendToList(subval);
        } else if (amqType == 0xa8) {
            FieldTable subFt;
            bool valid = qpid::framing::getEncodedValue<FieldTable>(*iter, subFt);
            if (valid) {
                Value* subval(new Value(TYPE_MAP));
                subval->impl->initMap(subFt);
                appendToList(subval);
            }
        } else if (amqType == 0xa9) {
            List subList;
            bool valid = qpid::framing::getEncodedValue<List>(*iter, subList);
            if (valid) {
                Value *subVal(new Value(TYPE_LIST));
                subVal->impl->initList(subList);
                appendToList(subVal);
            }
        } else if (amqType == 0x08) {
            Value* subval(new Value(TYPE_BOOL));
            subval->setBool(fvalue.get<int>() ? true : false);
            appendToList(subval);
        } else {
            QPID_LOG(error, "Unable to decode unsupported AMQP typecode =" << amqType);
        }
    }
}

void ValueImpl::listToFramingList(List& fl) const
{
    for (vector<Value>::const_iterator iter = vectorVal.begin();
         iter != vectorVal.end(); iter++) {
        const Value& subval(*iter);

        switch (subval.getType()) {
        case TYPE_UINT8:
        case TYPE_UINT16:
        case TYPE_UINT32:
            fl.push_back(List::ValuePtr(new Unsigned64Value((uint64_t) subval.asUint())));
            break;
        case TYPE_UINT64:
        case TYPE_DELTATIME:
            fl.push_back(List::ValuePtr(new Unsigned64Value(subval.asUint64())));
            break;
        case TYPE_SSTR:
        case TYPE_LSTR:
            fl.push_back(List::ValuePtr(new Str16Value(subval.asString())));
            break;
        case TYPE_INT64:
        case TYPE_ABSTIME:
            fl.push_back(List::ValuePtr(new Integer64Value(subval.asInt64())));
            break;
        case TYPE_BOOL:
            fl.push_back(List::ValuePtr(new BoolValue(subval.asBool() ? 1 : 0)));
            break;
        case TYPE_FLOAT:
            fl.push_back(List::ValuePtr(new FloatValue(subval.asFloat())));
            break;
        case TYPE_DOUBLE:
            fl.push_back(List::ValuePtr(new DoubleValue(subval.asDouble())));
            break;
        case TYPE_INT8:
        case TYPE_INT16:
        case TYPE_INT32:
            fl.push_back(List::ValuePtr(new IntegerValue(subval.asInt())));
            break;
        case TYPE_MAP:
            {
                FieldTable subFt;
                subval.impl->mapToFieldTable(subFt);
                fl.push_back(List::ValuePtr(new FieldTableValue(subFt)));

            } break;
        case TYPE_LIST:
            {
                List subList;
                subval.impl->listToFramingList(subList);
                fl.push_back(List::ValuePtr(new ListValue(subList)));
            } break;

        case TYPE_ARRAY:
        case TYPE_OBJECT:
        case TYPE_UUID:
        case TYPE_REF:
        default:
            break;
        }
    }
 }



void ValueImpl::encode(Buffer& buf) const
{
    FieldTable ft;
    List fl;

    switch (typecode) {
    case TYPE_UINT8     : buf.putOctet((uint8_t) value.u32);        break;
    case TYPE_UINT16    : buf.putShort((uint16_t) value.u32);       break;
    case TYPE_UINT32    : buf.putLong(value.u32);                   break;
    case TYPE_UINT64    : buf.putLongLong(value.u64);               break;
    case TYPE_SSTR      : buf.putShortString(stringVal);            break;
    case TYPE_LSTR      : buf.putMediumString(stringVal);           break;
    case TYPE_ABSTIME   : buf.putLongLong(value.s64);               break;
    case TYPE_DELTATIME : buf.putLongLong(value.u64);               break;
    case TYPE_BOOL      : buf.putOctet(value.boolVal ? 1 : 0);      break;
    case TYPE_FLOAT     : buf.putFloat(value.floatVal);             break;
    case TYPE_DOUBLE    : buf.putDouble(value.doubleVal);           break;
    case TYPE_INT8      : buf.putOctet((uint8_t) value.s32);        break;
    case TYPE_INT16     : buf.putShort((uint16_t) value.s32);       break;
    case TYPE_INT32     : buf.putLong(value.s32);                   break;
    case TYPE_INT64     : buf.putLongLong(value.s64);               break;
    case TYPE_UUID      : buf.putBin128(value.uuidVal);             break;
    case TYPE_REF       : refVal.impl->encode(buf);                 break;
    case TYPE_MAP:
        mapToFieldTable(ft);
        ft.encode(buf);
        break;
    case TYPE_LIST:
        listToFramingList(fl);
        fl.encode(buf);
        break;

    case TYPE_ARRAY:
    case TYPE_OBJECT:
    default:
        break;
    }
}

uint32_t ValueImpl::encodedSize() const
{
    FieldTable ft;
    List fl;

    switch (typecode) {
    case TYPE_UINT8     :
    case TYPE_BOOL      :
    case TYPE_INT8      : return 1;
 
    case TYPE_UINT16    :
    case TYPE_INT16     : return 2;

    case TYPE_UINT32    :
    case TYPE_INT32     :
    case TYPE_FLOAT     : return 4;

    case TYPE_UINT64    :
    case TYPE_INT64     :
    case TYPE_DOUBLE    :
    case TYPE_ABSTIME   :
    case TYPE_DELTATIME : return 8;

    case TYPE_UUID      : 
    case TYPE_REF       : return 16;

    case TYPE_SSTR      : return 1 + stringVal.size();
    case TYPE_LSTR      : return 2 + stringVal.size();
    case TYPE_MAP:
        mapToFieldTable(ft);
        return ft.encodedSize();

    case TYPE_LIST:
        listToFramingList(fl);
        return fl.encodedSize();

    case TYPE_ARRAY:
    case TYPE_OBJECT:
    default:
        break;
    }

    return 0;
}

bool ValueImpl::keyInMap(const char* key) const
{
    return typecode == TYPE_MAP && mapVal.count(key) > 0;
}

Value* ValueImpl::byKey(const char* key)
{
    if (keyInMap(key)) {
        map<string, Value>::iterator iter = mapVal.find(key);
        if (iter != mapVal.end())
            return &iter->second;
    }
    return 0;
}

const Value* ValueImpl::byKey(const char* key) const
{
    if (keyInMap(key)) {
        map<string, Value>::const_iterator iter = mapVal.find(key);
        if (iter != mapVal.end())
            return &iter->second;
    }
    return 0;
}

void ValueImpl::deleteKey(const char* key)
{
    mapVal.erase(key);
}

void ValueImpl::insert(const char* key, Value* val)
{
    pair<string, Value> entry(key, *val);
    mapVal.insert(entry);
}

const char* ValueImpl::key(uint32_t idx) const
{
    map<string, Value>::const_iterator iter = mapVal.begin();
    for (uint32_t i = 0; i < idx; i++) {
        if (iter == mapVal.end())
            break;
        iter++;
    }

    if (iter == mapVal.end())
        return 0;
    else
        return iter->first.c_str();
}

Value* ValueImpl::arrayItem(uint32_t)
{
    return 0;
}

void ValueImpl::appendToArray(Value*)
{
}

void ValueImpl::deleteArrayItem(uint32_t)
{
}


//==================================================================
// Wrappers
//==================================================================

Value::Value(const Value& from) : impl(new ValueImpl(*(from.impl))) {}
Value::Value(Typecode t, Typecode at) : impl(new ValueImpl(t, at)) {}
Value::Value(ValueImpl* i) : impl(i) {}
Value::~Value() { delete impl;}

Typecode Value::getType() const { return impl->getType(); }
bool Value::isNull() const { return impl->isNull(); }
void Value::setNull() { impl->setNull(); }
bool Value::isObjectId() const { return impl->isObjectId(); }
const ObjectId& Value::asObjectId() const { return impl->asObjectId(); }
void Value::setObjectId(const ObjectId& oid) { impl->setObjectId(oid); }
bool Value::isUint() const { return impl->isUint(); }
uint32_t Value::asUint() const { return impl->asUint(); }
void Value::setUint(uint32_t val) { impl->setUint(val); }
bool Value::isInt() const { return impl->isInt(); }
int32_t Value::asInt() const { return impl->asInt(); }
void Value::setInt(int32_t val) { impl->setInt(val); }
bool Value::isUint64() const { return impl->isUint64(); }
uint64_t Value::asUint64() const { return impl->asUint64(); }
void Value::setUint64(uint64_t val) { impl->setUint64(val); }
bool Value::isInt64() const { return impl->isInt64(); }
int64_t Value::asInt64() const { return impl->asInt64(); }
void Value::setInt64(int64_t val) { impl->setInt64(val); }
bool Value::isString() const { return impl->isString(); }
const char* Value::asString() const { return impl->asString(); }
void Value::setString(const char* val) { impl->setString(val); }
bool Value::isBool() const { return impl->isBool(); }
bool Value::asBool() const { return impl->asBool(); }
void Value::setBool(bool val) { impl->setBool(val); }
bool Value::isFloat() const { return impl->isFloat(); }
float Value::asFloat() const { return impl->asFloat(); }
void Value::setFloat(float val) { impl->setFloat(val); }
bool Value::isDouble() const { return impl->isDouble(); }
double Value::asDouble() const { return impl->asDouble(); }
void Value::setDouble(double val) { impl->setDouble(val); }
bool Value::isUuid() const { return impl->isUuid(); }
const uint8_t* Value::asUuid() const { return impl->asUuid(); }
void Value::setUuid(const uint8_t* val) { impl->setUuid(val); }
bool Value::isObject() const { return impl->isObject(); }
const Object* Value::asObject() const { return impl->asObject(); }
void Value::setObject(Object* val) { impl->setObject(val); }
bool Value::isMap() const { return impl->isMap(); }
bool Value::keyInMap(const char* key) const { return impl->keyInMap(key); }
Value* Value::byKey(const char* key) { return impl->byKey(key); }
const Value* Value::byKey(const char* key) const { return impl->byKey(key); }
void Value::deleteKey(const char* key) { impl->deleteKey(key); }
void Value::insert(const char* key, Value* val) { impl->insert(key, val); }
uint32_t Value::keyCount() const { return impl->keyCount(); }
const char* Value::key(uint32_t idx) const { return impl->key(idx); }
bool Value::isList() const { return impl->isList(); }
uint32_t Value::listItemCount() const { return impl->listItemCount(); }
Value* Value::listItem(uint32_t idx) { return impl->listItem(idx); }
void Value::appendToList(Value* val) { impl->appendToList(val); }
void Value::deleteListItem(uint32_t idx) { impl->deleteListItem(idx); }
bool Value::isArray() const { return impl->isArray(); }
Typecode Value::arrayType() const { return impl->arrayType(); }
uint32_t Value::arrayItemCount() const { return impl->arrayItemCount(); }
Value* Value::arrayItem(uint32_t idx) { return impl->arrayItem(idx); }
void Value::appendToArray(Value* val) { impl->appendToArray(val); }
void Value::deleteArrayItem(uint32_t idx) { impl->deleteArrayItem(idx); }

