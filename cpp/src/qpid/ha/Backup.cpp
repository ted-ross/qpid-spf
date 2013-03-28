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
#include "Backup.h"
#include "BrokerReplicator.h"
#include "ConnectionObserver.h"
#include "HaBroker.h"
#include "Primary.h"
#include "ReplicatingSubscription.h"
#include "Settings.h"
#include "StatusCheck.h"
#include "qpid/Url.h"
#include "qpid/amqp_0_10/Codecs.h"
#include "qpid/broker/Bridge.h"
#include "qpid/broker/Broker.h"
#include "qpid/broker/SessionHandler.h"
#include "qpid/broker/Link.h"
#include "qpid/framing/AMQP_ServerProxy.h"
#include "qpid/framing/AMQFrame.h"
#include "qpid/framing/FieldTable.h"
#include "qpid/framing/MessageTransferBody.h"
#include "qpid/sys/SystemInfo.h"
#include "qpid/types/Variant.h"
#include "qpid/log/Statement.h"

namespace qpid {
namespace ha {

using namespace framing;
using namespace broker;
using types::Variant;
using std::string;
using sys::Mutex;

Backup::Backup(HaBroker& hb, const Settings& s) :
    logPrefix("Backup: "), membership(hb.getMembership()), stopped(false),
    haBroker(hb), broker(hb.getBroker()), settings(s),
    statusCheck(
        new StatusCheck(
            logPrefix, broker.getLinkHearbeatInterval(), hb.getBrokerInfo()))
{
    // Set link properties to tag outgoing links.
    framing::FieldTable linkProperties = broker.getLinkClientProperties();
    linkProperties.setTable(
        ConnectionObserver::BACKUP_TAG, hb.getBrokerInfo().asFieldTable());
    broker.setLinkClientProperties(linkProperties);
}

void Backup::setBrokerUrl(const Url& brokers) {
    if (brokers.empty()) return;
    Mutex::ScopedLock l(lock);
    if (stopped) return;
    if (haBroker.getStatus() == JOINING) statusCheck->setUrl(brokers);
    if (!link) {                // Not yet initialized
        QPID_LOG(info, logPrefix << "Connecting to cluster, broker URL: " << brokers);
        string protocol = brokers[0].protocol.empty() ? "tcp" : brokers[0].protocol;
        types::Uuid uuid(true);
        std::pair<Link::shared_ptr, bool> result;
        result = broker.getLinks().declare(
            broker::QPID_NAME_PREFIX + string("ha.link.") + uuid.str(),
            brokers[0].host, brokers[0].port, protocol,
            false,                  // durable
            settings.mechanism, settings.username, settings.password,
            false);               // no amq.failover - don't want to use client URL.
        link = result.first;
        replicator.reset(new BrokerReplicator(haBroker, link));
        replicator->initialize();
        broker.getExchanges().registerExchange(replicator);
    }
    link->setUrl(brokers);          // Outside the lock, once set link doesn't change.
}

void Backup::stop(Mutex::ScopedLock&) {
    if (stopped) return;
    stopped = true;
    QPID_LOG(debug, logPrefix << "Leaving backup role.");
    if (link) link->close();
    if (replicator.get()) {
        replicator->shutdown();
        replicator.reset();
    }
}

Role* Backup::recover(Mutex::ScopedLock&) {
    BrokerInfo::Set backups;
    {
        Mutex::ScopedLock l(lock);
        if (stopped) return 0;
        stop(l);                 // Stop backup activity before starting primary.
        QPID_LOG(notice, "Promoting to primary: " << haBroker.getBrokerInfo());
        // Reset membership before allowing backups to connect.
        backups = membership.otherBackups();
        membership.clear();
        return new Primary(haBroker, backups);
    }
}

Role* Backup::promote() {
    Mutex::ScopedLock l(lock);
    if (stopped) return 0;
    switch (haBroker.getStatus()) {
      case JOINING:
        if (statusCheck->canPromote()) return recover(l);
        else {
            QPID_LOG(error,
                     logPrefix << "Joining active cluster, cannot be promoted.");
            throw Exception("Joining active cluster, cannot be promoted.");
        }
        break;
      case CATCHUP:
        QPID_LOG(error, logPrefix << "Still catching up, cannot be promoted.");
        throw Exception("Still catching up, cannot be promoted.");
        break;
      case READY: return recover(l); break;
      default:
        assert(0);              // Not a valid state for the Backup role..
    }
    return 0; 			// Keep compiler happy
}

Backup::~Backup() {
    Mutex::ScopedLock l(lock);
    stop(l);
}

}} // namespace qpid::ha
