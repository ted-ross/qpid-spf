/*
 *
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
 *
 */
#ifndef _SpfExchange_
#define _SpfExchange_

#include "qpid/broker/BrokerImportExport.h"
#include "qpid/broker/TopicExchange.h"
#include <memory>
#include <set>

namespace qpid {
namespace spf {

    class Router;

    class SpfExchange : public virtual qpid::broker::TopicExchange {
    public:
        static const std::string typeName;

        SpfExchange(const std::string& name,
                    management::Manageable* parent = 0, qpid::broker::Broker* broker = 0);
        SpfExchange(const std::string& _name,
                    bool _durable,
                    const qpid::framing::FieldTable& _args,
                    management::Manageable* parent = 0, qpid::broker::Broker* broker = 0);

        virtual std::string getType() const { return typeName; }

        virtual bool bind(qpid::broker::Queue::shared_ptr queue,
                          const std::string& routingKey,
                          const qpid::framing::FieldTable* args);
        virtual void localBind(const std::string& routingKey);

        virtual bool unbind(qpid::broker::Queue::shared_ptr queue,
                            const std::string& routingKey,
                            const qpid::framing::FieldTable* args);
        virtual void localUnbind(const std::string& routingKey);

        virtual void route(qpid::broker::Deliverable& msg);
        virtual void routeOutbound(qpid::broker::Deliverable& msg);

        virtual bool isBound(qpid::broker::Queue::shared_ptr queue,
                             const std::string* const routingKey,
                             const qpid::framing::FieldTable* const args);

        virtual ~SpfExchange();
        virtual bool supportsDynamicBinding() { return false; }
        void setupRouter(const std::string& name);
        void reprocessHeldMessages();

    private:
        sys::Mutex routerLock;
        std::set<std::string> routerBindings;
        std::auto_ptr<Router> router;
        boost::shared_ptr<qpid::broker::Queue> holdingQueue;
    };

}
}

#endif
