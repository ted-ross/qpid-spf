#ifndef QPID_ACL_ACLVALIDATOR_H
#define QPID_ACL_ACLVALIDATOR_H


/*
 *
 * Copyright (c) 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "qpid/broker/AclModule.h"
#include "qpid/acl/AclData.h"
#include "qpid/sys/IntegerTypes.h"
#include <boost/shared_ptr.hpp>
#include <vector>
#include <sstream>

namespace qpid {
namespace acl {

class AclValidator {

    /* Base Property */
   class PropertyType{

        public:
            virtual ~PropertyType(){};
            virtual bool validate(const std::string& val)=0;
            virtual std::string allowedValues()=0;
   };

   class IntPropertyType : public PropertyType{
            int64_t min;
            int64_t max;

        public:
            IntPropertyType(int64_t min,int64_t max);
            virtual ~IntPropertyType (){};
            virtual bool validate(const std::string& val);
            virtual std::string allowedValues();
   };

   class EnumPropertyType : public PropertyType{
            std::vector<std::string> values;

        public:
            EnumPropertyType(std::vector<std::string>& allowed);
            virtual ~EnumPropertyType (){};
            virtual bool validate(const std::string& val);
            virtual std::string allowedValues();
   };

   typedef std::pair<acl::SpecProperty,boost::shared_ptr<PropertyType> > Validator;
   typedef std::map<acl::SpecProperty,boost::shared_ptr<PropertyType> > ValidatorMap;
   typedef ValidatorMap::iterator ValidatorItr;

   ValidatorMap validators;

public:

   void validateRuleSet(std::pair<const std::string, qpid::acl::AclData::ruleSet>& rules);
   void validateRule(qpid::acl::AclData::Rule& rule);
   void validateProperty(std::pair<const qpid::acl::SpecProperty, std::string>& prop);
   void validate(boost::shared_ptr<AclData> d);
   AclValidator();
   ~AclValidator();
};

}} // namespace qpid::acl

#endif // QPID_ACL_ACLVALIDATOR_H
