
#ifndef _MANAGEMENT_CHILDDESTROYED_
#define _MANAGEMENT_CHILDDESTROYED_

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

#include "qpid/management/ManagementEvent.h"


namespace qmf {
namespace org {
namespace apache {
namespace qpid {
namespace agent {
namespace example {


 class EventChildDestroyed : public ::qpid::management::ManagementEvent
{
  private:
    static void writeSchema (std::string& schema);
    static uint8_t md5Sum[MD5_LEN];
     static std::string packageName;
     static std::string eventName;

    const std::string& childName;


  public:
    writeSchemaCall_t getWriteSchemaCall(void) { return writeSchema; }

     EventChildDestroyed(const std::string& _childName);
     ~EventChildDestroyed() {};

    static void registerSelf(::qpid::management::ManagementAgent* agent);
    std::string& getPackageName() const { return packageName; }
    std::string& getEventName() const { return eventName; }
    uint8_t* getMd5Sum() const { return md5Sum; }
    uint8_t getSeverity() const { return 6; }
     void encode(std::string& buffer) const;
     void mapEncode(::qpid::types::Variant::Map& map) const;

     static bool match(const std::string& evt, const std::string& pkg);
    static std::pair<std::string,std::string> getFullName() {
        return std::make_pair(packageName, eventName);
    }
};

}}}}}}

#endif  /*!_MANAGEMENT_CHILDDESTROYED_*/
