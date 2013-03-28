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

#include <Python.h>
#include "qpid/spf/SpfExchange.h"
#include "qpid/spf/Router.h"
#include "qpid/log/Statement.h"
#include "qpid/broker/Broker.h"
#include "qpid/broker/ExchangeRegistry.h"
#include "qpid/broker/QueueRegistry.h"
#include "qpid/broker/QueueSettings.h"
#include "qpid/framing/reply_exceptions.h"
#include <sstream>

using namespace qpid::spf;
using namespace qpid::framing;
using namespace qpid::sys;
using namespace std;

SpfExchange::SpfExchange(const string& _name, Manageable* _parent, qpid::broker::Broker* b) :
    qpid::broker::TopicExchange(_name, _parent, b), router(new Router(_name, *this, "spfrouter"))
{
    if (mgmtExchange != 0)
        mgmtExchange->set_type(typeName);
}


SpfExchange::SpfExchange(const std::string& _name, bool _durable,
                         const FieldTable& _args, Manageable* _parent, qpid::broker::Broker* b) :
    qpid::broker::TopicExchange(_name, _durable, _args, _parent, b), router(new Router(_name, *this, "spfrouter", _args))
{
    if (mgmtExchange != 0)
        mgmtExchange->set_type(typeName);
}


void SpfExchange::setupRouter(const std::string& domain)
{
    //
    // Create matching fanout exchange as an alternate for unroutable messages
    //
    stringstream unroutable;
    unroutable << domain << "_unroutable";
    pair<boost::shared_ptr<qpid::broker::Exchange>, bool> unroutableExchange(broker->getExchanges().declare(unroutable.str(), "fanout"));
    setAlternate(unroutableExchange.first);
    QPID_LOG(notice, "SPF: Declared fanout exchange for unroutable messages: " << unroutable.str());

    //
    // Create a holding queue to hold unroutable messages and bind it to the unroutable exchange
    //
    qpid::broker::QueueSettings settings(false, false);
    boost::shared_ptr<Exchange> thisExchange((broker->getExchanges().find(domain)));
    stringstream holding;
    holding << "spf_holding_" << domain;
    pair<boost::shared_ptr<qpid::broker::Queue>, bool> 
        hqpair(broker->getQueues().declare(holding.str(), settings, thisExchange));
    holdingQueue = hqpair.first;
    holdingQueue->bind(unroutableExchange.first, "key", qpid::framing::FieldTable());
    QPID_LOG(notice, "SPF: Declared holding queue for unroutable messages: " << holding.str());
}


void SpfExchange::reprocessHeldMessages()
{
    if (holdingQueue.get())
        holdingQueue->purge(0, holdingQueue->getAlternateExchange());
}


bool SpfExchange::bind(qpid::broker::Queue::shared_ptr queue, const string& routingKey, const FieldTable* args)
{
    bool result = qpid::broker::TopicExchange::bind(queue, routingKey, args);
    if (result && queue->getName().find("spf_") != 0)
        router->bindingAdded(routingKey);
    return result;
}


bool SpfExchange::unbind(qpid::broker::Queue::shared_ptr queue, const string& constRoutingKey, const FieldTable* args)
{
    if (queue->getName().find("spf_") != 0)
        router->bindingDeleted(constRoutingKey);
    return qpid::broker::TopicExchange::unbind(queue, constRoutingKey, args);
}


void SpfExchange::localBind(const std::string& routingKey)
{
    Mutex::ScopedLock l(routerLock);
    routerBindings.insert(routingKey);
}


void SpfExchange::localUnbind(const std::string& routingKey)
{
    Mutex::ScopedLock l(routerLock);
    routerBindings.erase(routingKey);
}


void SpfExchange::route(qpid::broker::Deliverable& msg)
{
    const string& routingKey = msg.getMessage().getRoutingKey();

    bool localMatch(false);
    {
        Mutex::ScopedLock l(routerLock);
        if (routerBindings.count(routingKey) > 0)
            localMatch = true;
    }

    if (localMatch) {
        router->handleControlMessage(msg);
        msg.delivered = true;
    }

    if (routingKey != "_peer")
        qpid::broker::TopicExchange::route(msg);
}


void SpfExchange::routeOutbound(qpid::broker::Deliverable& msg)
{
    //
    // Simply route through the topic exchange.  Don't inspect the message for possible inbound
    // processing.  This is used for messages that originate from this router.
    //
    qpid::broker::TopicExchange::route(msg);

    if (!msg.delivered && getAlternate())
        getAlternate()->route(msg);
}


bool SpfExchange::isBound(qpid::broker::Queue::shared_ptr queue, const string* const routingKey, const FieldTable* const args)
{
    return qpid::broker::TopicExchange::isBound(queue, routingKey, args);
}


SpfExchange::~SpfExchange()
{
    //
    // This method intentionally left blank
    //
}


const std::string SpfExchange::typeName("spf");

