#ifndef QPID_HA_REMOTEBACKUP_H
#define QPID_HA_REMOTEBACKUP_H

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

#include "ReplicationTest.h"
#include "BrokerInfo.h"
#include "types.h"
#include <set>
#include <map>

namespace qpid {

namespace broker {
class Queue;
class QueueRegistry;
class Connection;
}

namespace ha {
class QueueGuard;

/**
 * Track readiness for a remote broker.
 * Creates queue guards on behalf of the remote broker to keep
 * queues safe till the ReplicatingSubscription is ready.
 *
 * THREAD UNSAFE: Caller must serialize.
 */
class RemoteBackup
{
  public:
    typedef boost::shared_ptr<QueueGuard> GuardPtr;
    typedef boost::shared_ptr<broker::Queue> QueuePtr;

    /** Note: isReady() can be true after construction
     *@param connected true if the backup is already connected.
     */
    RemoteBackup(const BrokerInfo&, broker::Connection*);
    ~RemoteBackup();

    /** Set all queues in the registry as catch-up queues.
     *@createGuards if true create guards also, if false guards are created on demand.
     */
    void setCatchupQueues(broker::QueueRegistry&, bool createGuards);

    /** Return guard associated with a queue. Used to create ReplicatingSubscription. */
    GuardPtr guard(const QueuePtr&);

    /** Is the remote backup connected? */
    void setConnection(broker::Connection* c) { connection = c; }
    bool isConnected() const { return connection; }

    /** ReplicatingSubscription associated with queue is ready.
     * Note: may set isReady()
     */
    void ready(const QueuePtr& queue);

    /** Called via ConfigurationObserver */
    void queueCreate(const QueuePtr&);

    /** Called via ConfigurationObserver. Note: may set isReady() */
    void queueDestroy(const QueuePtr&);

    /**@return true when all catch-up queues for this backup are ready. */
    bool isReady();

    /**@return true if isReady() and this is the first call to reportReady */
    bool reportReady();

    /**Cancel all queue guards, called if we are timed out. */
    void cancel();

    BrokerInfo getBrokerInfo() const { return brokerInfo; }
  private:
    typedef std::map<QueuePtr, GuardPtr> GuardMap;
    typedef std::set<QueuePtr> QueueSet;

    void catchupQueue(const QueuePtr&, bool createGuard);

    std::string logPrefix;
    BrokerInfo brokerInfo;
    ReplicationTest replicationTest;
    GuardMap guards;
    QueueSet catchupQueues;
    broker::Connection* connection;
    bool reportedReady;
};

}} // namespace qpid::ha

#endif  /*!QPID_HA_REMOTEBACKUP_H*/
