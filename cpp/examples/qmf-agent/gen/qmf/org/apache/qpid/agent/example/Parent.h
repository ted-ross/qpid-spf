
#ifndef _MANAGEMENT_ORG_APACHE_QPID_AGENT_EXAMPLE_PARENT_
#define _MANAGEMENT_ORG_APACHE_QPID_AGENT_EXAMPLE_PARENT_

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

#include "qpid/management/ManagementObject.h"

#include <limits>

namespace qpid {
    namespace management {
        class ManagementAgent;
    }
}

namespace qmf {
namespace org {
namespace apache {
namespace qpid {
namespace agent {
namespace example {


 class Parent : public ::qpid::management::ManagementObject
{
  private:

    static std::string packageName;
    static std::string className;
    static uint8_t     md5Sum[MD5_LEN];


    // Properties
    std::string name;
    ::qpid::types::Variant::Map args;
    ::qpid::types::Variant::List list;

    // Statistics
    std::string  state;


    // Per-Thread Statistics

 public:    
    struct PerThreadStats {
        uint64_t  count;

    };
 private:

    struct PerThreadStats** perThreadStatsArray;

    inline struct PerThreadStats* getThreadStats() {
        int idx = getThreadIndex();
        struct PerThreadStats* threadStats = perThreadStatsArray[idx];
        if (threadStats == 0) {
            threadStats = new(PerThreadStats);
            perThreadStatsArray[idx] = threadStats;
            threadStats->count = 0;

        }
        return threadStats;
    }

    void aggregatePerThreadStats(struct PerThreadStats*) const;

  public:


     static void writeSchema(std::string& schema);
     void mapEncodeValues(::qpid::types::Variant::Map& map,
                                          bool includeProperties=true,
                                          bool includeStatistics=true);
     void mapDecodeValues(const ::qpid::types::Variant::Map& map);
     void doMethod(std::string&           methodName,
                                   const ::qpid::types::Variant::Map& inMap,
                                   ::qpid::types::Variant::Map& outMap,
                                   const std::string& userId);
     std::string getKey() const;


    writeSchemaCall_t getWriteSchemaCall() { return writeSchema; }


     Parent(
        ::qpid::management::ManagementAgent* agent,
        ::qpid::management::Manageable* coreObject, const std::string& _name);

     ~Parent();

    

     static void registerSelf(
        ::qpid::management::ManagementAgent* agent);

    std::string& getPackageName() const { return packageName; }
    std::string& getClassName() const { return className; }
    uint8_t* getMd5Sum() const { return md5Sum; }

    // Method IDs
    static const uint32_t METHOD_CREATE_CHILD = 1;
    static const uint32_t METHOD_TEST_METHOD = 2;
    static const uint32_t METHOD_AUTH_FAIL = 3;

    // Accessor Methods
    inline void set_args (const ::qpid::types::Variant::Map& val) {
        ::qpid::management::Mutex::ScopedLock mutex(accessLock);
        args = val;
        configChanged = true;
    }
    inline const ::qpid::types::Variant::Map& get_args() {
        ::qpid::management::Mutex::ScopedLock mutex(accessLock);
        return args;
    }
    inline void set_list (const ::qpid::types::Variant::List& val) {
        ::qpid::management::Mutex::ScopedLock mutex(accessLock);
        list = val;
        configChanged = true;
    }
    inline const ::qpid::types::Variant::List& get_list() {
        ::qpid::management::Mutex::ScopedLock mutex(accessLock);
        return list;
    }
    inline void set_state (const std::string& val) {
        ::qpid::management::Mutex::ScopedLock mutex(accessLock);
        state = val;
        instChanged = true;
    }
    inline const std::string& get_state() {
        ::qpid::management::Mutex::ScopedLock mutex(accessLock);
        return state;
    }
    inline void inc_count (uint64_t by = 1) {
        getThreadStats()->count += by;
        instChanged = true;
    }
    inline void dec_count (uint64_t by = 1) {
        getThreadStats()->count -= by;
        instChanged = true;
    }



    struct PerThreadStats* getStatistics() { return getThreadStats(); }
    void statisticsUpdated() { instChanged = true; }

};

}}}}}}

#endif  /*!_MANAGEMENT_PARENT_*/
