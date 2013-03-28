
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

// This source file was created by a code generator.
// Please do not edit.

#include "qpid/management/Manageable.h"
#include "qpid/management/Buffer.h"
#include "qpid/types/Variant.h"
#include "qpid/amqp_0_10/Codecs.h"
#include "qpid/agent/ManagementAgent.h"
#include "Parent.h"
#include "ArgsParentCreate_child.h"
#include "ArgsParentTest_method.h"


#include <iostream>
#include <sstream>
#include <string.h>

using namespace qmf::org::apache::qpid::agent::example;
using           qpid::management::ManagementAgent;
using           qpid::management::Manageable;
using           qpid::management::ManagementObject;
using           qpid::management::Args;
using           qpid::management::Mutex;
using           std::string;

string  Parent::packageName  = string ("org.apache.qpid.agent.example");
string  Parent::className    = string ("parent");
uint8_t Parent::md5Sum[MD5_LEN]   =
    {0xe5,0x2b,0xff,0x3a,0xae,0xb5,0x74,0x62,0x9,0xe9,0x39,0xe6,0xce,0x27,0xe9,0xf0};

Parent::Parent (ManagementAgent*, Manageable* _core, const std::string& _name) :
    ManagementObject(_core),name(_name)
{
    
    args = ::qpid::types::Variant::Map();
    list = ::qpid::types::Variant::List();
    state = "";



    perThreadStatsArray = new struct PerThreadStats*[maxThreads];
    for (int idx = 0; idx < maxThreads; idx++)
        perThreadStatsArray[idx] = 0;


}

Parent::~Parent ()
{


    for (int idx = 0; idx < maxThreads; idx++)
        if (perThreadStatsArray[idx] != 0)
            delete perThreadStatsArray[idx];
    delete[] perThreadStatsArray;

}

namespace {
    const string NAME("name");
    const string TYPE("type");
    const string ACCESS("access");
    const string IS_INDEX("index");
    const string IS_OPTIONAL("optional");
    const string UNIT("unit");
    const string MIN("min");
    const string MAX("max");
    const string MAXLEN("maxlen");
    const string DESC("desc");
    const string ARGCOUNT("argCount");
    const string ARGS("args");
    const string DIR("dir");
    const string DEFAULT("default");
}

void Parent::registerSelf(ManagementAgent* agent)
{
    agent->registerClass(packageName, className, md5Sum, writeSchema);
}

void Parent::writeSchema (std::string& schema)
{
    const int _bufSize=65536;
    char _msgChars[_bufSize];
    ::qpid::management::Buffer buf(_msgChars, _bufSize);
    ::qpid::types::Variant::Map ft;

    // Schema class header:
    buf.putOctet       (CLASS_KIND_TABLE);
    buf.putShortString (packageName); // Package Name
    buf.putShortString (className);   // Class Name
    buf.putBin128      (md5Sum);      // Schema Hash
    buf.putShort       (3); // Config Element Count
    buf.putShort       (2); // Inst Element Count
    buf.putShort       (3); // Method Count

    // Properties
    ft.clear();
    ft[NAME] = "name";
    ft[TYPE] = TYPE_LSTR;
    ft[ACCESS] = ACCESS_RC;
    ft[IS_INDEX] = 1;
    ft[IS_OPTIONAL] = 0;
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "args";
    ft[TYPE] = TYPE_FTABLE;
    ft[ACCESS] = ACCESS_RO;
    ft[IS_INDEX] = 0;
    ft[IS_OPTIONAL] = 0;
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "list";
    ft[TYPE] = TYPE_LIST;
    ft[ACCESS] = ACCESS_RO;
    ft[IS_INDEX] = 0;
    ft[IS_OPTIONAL] = 0;
    buf.putMap(ft);


    // Statistics
    ft.clear();
    ft[NAME] = "state";
    ft[TYPE] = TYPE_SSTR;
    ft[DESC] = "Operational state of the link";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "count";
    ft[TYPE] = TYPE_U64;
    ft[UNIT] = "tick";
    ft[DESC] = "Counter that increases monotonically";
    buf.putMap(ft);


    // Methods
    ft.clear();
    ft[NAME] =  "create_child";
    ft[ARGCOUNT] = 2;
    ft[DESC] = "Create child object";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "name";
    ft[TYPE] = TYPE_LSTR;
    ft[DIR] = "I";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "childRef";
    ft[TYPE] = TYPE_REF;
    ft[DIR] = "O";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] =  "test_method";
    ft[ARGCOUNT] = 2;
    ft[DESC] = "Test Method with Map and List Arguments";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "aMap";
    ft[TYPE] = TYPE_FTABLE;
    ft[DIR] = "IO";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] = "aList";
    ft[TYPE] = TYPE_LIST;
    ft[DIR] = "IO";
    buf.putMap(ft);

    ft.clear();
    ft[NAME] =  "auth_fail";
    ft[ARGCOUNT] = 0;
    ft[DESC] = "Method that fails authorization";
    buf.putMap(ft);


    {
        uint32_t _len = buf.getPosition();
        buf.reset();
        buf.getRawData(schema, _len);
    }
}


void Parent::aggregatePerThreadStats(struct PerThreadStats* totals) const
{
    totals->count = 0;

    for (int idx = 0; idx < maxThreads; idx++) {
        struct PerThreadStats* threadStats = perThreadStatsArray[idx];
        if (threadStats != 0) {
            totals->count += threadStats->count;

        }
    }
}



std::string Parent::getKey() const
{
    std::stringstream key;

    key << name;
    return key.str();
}


void Parent::mapEncodeValues (::qpid::types::Variant::Map& _map,
                                              bool includeProperties,
                                              bool includeStatistics)
{
    using namespace ::qpid::types;
    Mutex::ScopedLock mutex(accessLock);

    if (includeProperties) {
        configChanged = false;
    _map["name"] = ::qpid::types::Variant(name);
    _map["args"] = ::qpid::types::Variant(args);
    _map["list"] = ::qpid::types::Variant(list);

    }

    if (includeStatistics) {
        instChanged = false;


        struct PerThreadStats totals;
        aggregatePerThreadStats(&totals);



    _map["state"] = ::qpid::types::Variant(state);
    _map["count"] = ::qpid::types::Variant(totals.count);


    // Maintenance of hi-lo statistics


    }
}

void Parent::mapDecodeValues (const ::qpid::types::Variant::Map& _map)
{
    ::qpid::types::Variant::Map::const_iterator _i;
    Mutex::ScopedLock mutex(accessLock);

    if ((_i = _map.find("name")) != _map.end()) {
        name = (_i->second).getString();
    } else {
        name = "";
    }
    if ((_i = _map.find("args")) != _map.end()) {
        args = (_i->second).asMap();
    } else {
        args = ::qpid::types::Variant::Map();
    }
    if ((_i = _map.find("list")) != _map.end()) {
        list = (_i->second).asList();
    } else {
        list = ::qpid::types::Variant::List();
    }

}

void Parent::doMethod (string& methodName, const ::qpid::types::Variant::Map& inMap, ::qpid::types::Variant::Map& outMap, const string& userId)
{
    Manageable::status_t status = Manageable::STATUS_UNKNOWN_METHOD;
    std::string          text;


    if (methodName == "create_child") {
        ArgsParentCreate_child ioArgs;
        ::qpid::types::Variant::Map::const_iterator _i;
        if ((_i = inMap.find("name")) != inMap.end()) {
            ioArgs.i_name = (_i->second).getString();
        } else {
            ioArgs.i_name = "";
        }
        bool allow = coreObject->AuthorizeMethod(METHOD_CREATE_CHILD, ioArgs, userId);
        if (allow)
            status = coreObject->ManagementMethod (METHOD_CREATE_CHILD, ioArgs, text);
        else
            status = Manageable::STATUS_FORBIDDEN;
        outMap["_status_code"] = (uint32_t) status;
        outMap["_status_text"] = ::qpid::management::Manageable::StatusText(status, text);
        outMap["childRef"] = ::qpid::types::Variant(ioArgs.o_childRef);
        return;
    }

    if (methodName == "test_method") {
        ArgsParentTest_method ioArgs;
        ::qpid::types::Variant::Map::const_iterator _i;
        if ((_i = inMap.find("aMap")) != inMap.end()) {
            ioArgs.io_aMap = (_i->second).asMap();
        } else {
            ioArgs.io_aMap = ::qpid::types::Variant::Map();
        }
        if ((_i = inMap.find("aList")) != inMap.end()) {
            ioArgs.io_aList = (_i->second).asList();
        } else {
            ioArgs.io_aList = ::qpid::types::Variant::List();
        }
        bool allow = coreObject->AuthorizeMethod(METHOD_TEST_METHOD, ioArgs, userId);
        if (allow)
            status = coreObject->ManagementMethod (METHOD_TEST_METHOD, ioArgs, text);
        else
            status = Manageable::STATUS_FORBIDDEN;
        outMap["_status_code"] = (uint32_t) status;
        outMap["_status_text"] = ::qpid::management::Manageable::StatusText(status, text);
        outMap["aMap"] = ::qpid::types::Variant(ioArgs.io_aMap);
        outMap["aList"] = ::qpid::types::Variant(ioArgs.io_aList);
        return;
    }

    if (methodName == "auth_fail") {
        ::qpid::management::ArgsNone ioArgs;
        bool allow = coreObject->AuthorizeMethod(METHOD_AUTH_FAIL, ioArgs, userId);
        if (allow)
            status = coreObject->ManagementMethod (METHOD_AUTH_FAIL, ioArgs, text);
        else
            status = Manageable::STATUS_FORBIDDEN;
        outMap["_status_code"] = (uint32_t) status;
        outMap["_status_text"] = ::qpid::management::Manageable::StatusText(status, text);
        return;
    }

    outMap["_status_code"] = (uint32_t) status;
    outMap["_status_text"] = Manageable::StatusText(status, text);
}
