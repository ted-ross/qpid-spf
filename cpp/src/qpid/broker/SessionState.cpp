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
#include "qpid/broker/SessionState.h"
#include "qpid/broker/Broker.h"
#include "qpid/broker/ConnectionState.h"
#include "qpid/broker/DeliverableMessage.h"
#include "qpid/broker/DeliveryRecord.h"
#include "qpid/broker/SessionManager.h"
#include "qpid/broker/SessionHandler.h"
#include "qpid/framing/AMQContentBody.h"
#include "qpid/framing/AMQHeaderBody.h"
#include "qpid/framing/AMQMethodBody.h"
#include "qpid/framing/MessageTransferBody.h"
#include "qpid/framing/reply_exceptions.h"
#include "qpid/framing/ServerInvoker.h"
#include "qpid/log/Statement.h"
#include "qpid/management/ManagementAgent.h"

#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

namespace qpid {
namespace broker {

using namespace framing;
using sys::Mutex;
using boost::intrusive_ptr;
using qpid::management::ManagementAgent;
using qpid::management::ManagementObject;
using qpid::management::Manageable;
using qpid::management::Args;
using qpid::sys::AbsTime;
//using qpid::sys::Timer;
namespace _qmf = qmf::org::apache::qpid::broker;

SessionState::SessionState(
    Broker& b, SessionHandler& h, const SessionId& id,
    const SessionState::Configuration& config)
    : qpid::SessionState(id, config),
      broker(b), handler(&h),
      semanticState(*this),
      adapter(semanticState),
      asyncCommandCompleter(new AsyncCommandCompleter(this))
{
    addManagementObject();
    attach(h);
}

void SessionState::addManagementObject() {
    if (GetManagementObject()) return; // Already added.
    Manageable* parent = broker.GetVhostObject ();
    if (parent != 0) {
        ManagementAgent* agent = getBroker().getManagementAgent();
        if (agent != 0) {
            mgmtObject = _qmf::Session::shared_ptr(new _qmf::Session
                (agent, this, parent, getId().getName()));
            mgmtObject->set_attached (0);
            mgmtObject->set_detachedLifespan (0);
            mgmtObject->clr_expireTime();
            agent->addObject(mgmtObject);
        }
    }
}

SessionState::~SessionState() {
    asyncCommandCompleter->cancel();
    semanticState.closed();
    if (mgmtObject != 0)
        mgmtObject->resourceDestroy ();
}

AMQP_ClientProxy& SessionState::getProxy() {
    assert(isAttached());
    return handler->getProxy();
}

uint16_t SessionState::getChannel() const {
    assert(isAttached());
    return handler->getChannel();
}

ConnectionState& SessionState::getConnection() {
    assert(isAttached());
    return handler->getConnection();
}

bool SessionState::isLocal(const ConnectionToken* t) const
{
    return isAttached() && &(handler->getConnection()) == t;
}

void SessionState::detach() {
    QPID_LOG(debug, getId() << ": detached on broker.");
    asyncCommandCompleter->detached();
    disableOutput();
    handler = 0;
    if (mgmtObject != 0)
        mgmtObject->set_attached  (0);
}

void SessionState::disableOutput()
{
    semanticState.detached(); //prevents further activateOutput calls until reattached
}

void SessionState::attach(SessionHandler& h) {
    QPID_LOG(debug, getId() << ": attached on broker.");
    handler = &h;
    if (mgmtObject != 0)
    {
        mgmtObject->set_attached (1);
        mgmtObject->set_connectionRef (h.getConnection().GetManagementObject()->getObjectId());
        mgmtObject->set_channelId (h.getChannel());
    }
    asyncCommandCompleter->attached();
}

void SessionState::abort() {
    if (isAttached())
        getConnection().outputTasks.abort();
}

void SessionState::activateOutput() {
    if (isAttached())
        getConnection().outputTasks.activateOutput();
}

ManagementObject::shared_ptr SessionState::GetManagementObject(void) const
{
    return mgmtObject;
}

Manageable::status_t SessionState::ManagementMethod (uint32_t methodId,
                                                     Args&    /*args*/,
                                                     std::string&  /*text*/)
{
    Manageable::status_t status = Manageable::STATUS_UNKNOWN_METHOD;

    switch (methodId)
    {
    case _qmf::Session::METHOD_DETACH :
        if (handler != 0) {
            handler->sendDetach();
        }
        status = Manageable::STATUS_OK;
        break;

    case _qmf::Session::METHOD_CLOSE :
        /*
          if (handler != 0)
          {
          handler->getConnection().closeChannel(handler->getChannel());
          }
          status = Manageable::STATUS_OK;
          break;
        */

    case _qmf::Session::METHOD_SOLICITACK :
    case _qmf::Session::METHOD_RESETLIFESPAN :
        status = Manageable::STATUS_NOT_IMPLEMENTED;
        break;
    }

    return status;
}

void SessionState::handleCommand(framing::AMQMethodBody* method, const SequenceNumber& id) {
    currentCommandComplete = true;      // assumed, can be overridden by invoker method (this sucks).
    Invoker::Result invocation = invoke(adapter, *method);
    if (currentCommandComplete) receiverCompleted(id);

    if (!invocation.wasHandled()) {
        throw NotImplementedException(QPID_MSG("Not implemented: " << *method));
    } else if (invocation.hasResult()) {
        getProxy().getExecution().result(id, invocation.getResult());
    }

    if (method->isSync() && currentCommandComplete) {
        sendAcceptAndCompletion();
    }
}

void SessionState::handleContent(AMQFrame& frame, const SequenceNumber& id)
{
    if (frame.getBof() && frame.getBos()) //start of frameset
        msgBuilder.start(id);
    intrusive_ptr<qpid::broker::amqp_0_10::MessageTransfer> msg(msgBuilder.getMessage());
    msgBuilder.handle(frame);
    if (frame.getEof() && frame.getEos()) {//end of frameset
        if (frame.getBof()) {
            //i.e this is a just a command frame, add a dummy header
            AMQFrame header((AMQHeaderBody()));
            header.setBof(false);
            header.setEof(false);
            msg->getFrames().append(header);
        }
        DeliverableMessage deliverable(Message(msg, msg), semanticState.getTxBuffer());
        if (broker.isTimestamping())
            deliverable.getMessage().setTimestamp();
        deliverable.getMessage().setPublisher(&getConnection());


        IncompleteIngressMsgXfer xfer(this, msg);
        msg->getIngressCompletion().begin();
        semanticState.route(deliverable.getMessage(), deliverable);
        msgBuilder.end();
        msg->getIngressCompletion().end(xfer);  // allows msg to complete xfer
    }
}

void SessionState::sendAcceptAndCompletion()
{
    if (!accepted.empty()) {
        getProxy().getMessage().accept(accepted);
        accepted.clear();
    }
    sendCompletion();
}

/** Invoked when the given inbound message is finished being processed
 * by all interested parties (eg. it is done being enqueued to all queues,
 * its credit has been accounted for, etc).  At this point, msg is considered
 * by this receiver as 'completed' (as defined by AMQP 0_10)
 */
void SessionState::completeRcvMsg(SequenceNumber id,
                                  bool requiresAccept,
                                  bool requiresSync)
{
    bool callSendCompletion = false;
    receiverCompleted(id);
    if (requiresAccept)
        // will cause msg's seq to appear in the next message.accept we send.
        accepted.add(id);

    // Are there any outstanding Execution.Sync commands pending the
    // completion of this msg?  If so, complete them.
    while (!pendingExecutionSyncs.empty() &&
           receiverGetIncomplete().front() >= pendingExecutionSyncs.front()) {
        const SequenceNumber id = pendingExecutionSyncs.front();
        pendingExecutionSyncs.pop();
        QPID_LOG(debug, getId() << ": delayed execution.sync " << id << " is completed.");
        receiverCompleted(id);
        callSendCompletion = true;   // likely peer is pending for this completion.
    }

    // if the sender has requested immediate notification of the completion...
    if (requiresSync || callSendCompletion) {
        sendAcceptAndCompletion();
    }
}

void SessionState::handleIn(AMQFrame& frame) {
    SequenceNumber commandId = receiverGetCurrent();
    //TODO: make command handling more uniform, regardless of whether
    //commands carry content.
    AMQMethodBody* m = frame.getMethod();
    if (m == 0 || m->isContentBearing()) {
        handleContent(frame, commandId);
    } else if (frame.getBof() && frame.getEof()) {
        handleCommand(frame.getMethod(), commandId);
    } else {
        throw InternalErrorException("Cannot handle multi-frame command segments yet");
    }
}

void SessionState::handleOut(AMQFrame& frame) {
    assert(handler);
    handler->out(frame);
}

DeliveryId SessionState::deliver(const qpid::broker::amqp_0_10::MessageTransfer& message,
                                 const std::string& destination, bool isRedelivered, uint64_t ttl, uint64_t timestamp,
                                 qpid::framing::message::AcceptMode acceptMode, qpid::framing::message::AcquireMode acquireMode,
                                 const qpid::types::Variant::Map& annotations, bool sync)
{
    uint32_t maxFrameSize = getConnection().getFrameMax();
    assert(senderGetCommandPoint().offset == 0);
    SequenceNumber commandId = senderGetCommandPoint().command;

    framing::AMQFrame method((framing::MessageTransferBody(framing::ProtocolVersion(), destination, acceptMode, acquireMode)));
    method.setEof(false);
    getProxy().getHandler().handle(method);
    message.sendHeader(getProxy().getHandler(), maxFrameSize, isRedelivered, ttl, timestamp, annotations);
    message.sendContent(getProxy().getHandler(), maxFrameSize);

    assert(senderGetCommandPoint() == SessionPoint(commandId+1, 0)); // Delivery has moved sendPoint.
    if (sync) {
        AMQP_ClientProxy::Execution& p(getProxy().getExecution());
        Proxy::ScopedSync s(p);
        p.sync();
    }
    return commandId;
}

void SessionState::sendCompletion() {
    handler->sendCompletion();
}

void SessionState::senderCompleted(const SequenceSet& commands) {
    qpid::SessionState::senderCompleted(commands);
    semanticState.completed(commands);
}

void SessionState::readyToSend() {
    QPID_LOG(debug, getId() << ": ready to send, activating output.");
    assert(handler);
    semanticState.attached();
}

Broker& SessionState::getBroker() { return broker; }

// Session resume is not fully implemented so it is useless to set a
// non-0 timeout.
void SessionState::setTimeout(uint32_t) { }

// Current received command is an execution.sync command.
// Complete this command only when all preceding commands have completed.
// (called via the invoker() in handleCommand() above)
void SessionState::addPendingExecutionSync()
{
    SequenceNumber syncCommandId = receiverGetCurrent();
    if (receiverGetIncomplete().front() < syncCommandId) {
        currentCommandComplete = false;
        pendingExecutionSyncs.push(syncCommandId);
        asyncCommandCompleter->flushPendingMessages();
        QPID_LOG(debug, getId() << ": delaying completion of execution.sync " << syncCommandId);
    }
}

/** factory for creating a reference-counted IncompleteIngressMsgXfer object
 * which will be attached to a message that will be completed asynchronously.
 */
boost::intrusive_ptr<AsyncCompletion::Callback>
SessionState::IncompleteIngressMsgXfer::clone()
{
    // Optimization: this routine is *only* invoked when the message needs to be asynchronously completed.
    // If the client is pending the message.transfer completion, flush now to force immediate write to journal.
    if (requiresSync)
        msg->flush();
    else {
        // otherwise, we need to track this message in order to flush it if an execution.sync arrives
        // before it has been completed (see flushPendingMessages())
        pending = true;
        completerContext->addPendingMessage(msg);
    }

    return boost::intrusive_ptr<SessionState::IncompleteIngressMsgXfer>(new SessionState::IncompleteIngressMsgXfer(*this));
}


/** Invoked by the asynchronous completer associated with a received
 * msg that is pending Completion.  May be invoked by the IO thread
 * (sync == true), or some external thread (!sync).
 */
void SessionState::IncompleteIngressMsgXfer::completed(bool sync)
{
    if (pending) completerContext->deletePendingMessage(id);
    if (!sync) {
        /** note well: this path may execute in any thread.  It is safe to access
         * the scheduledCompleterContext, since *this has a shared pointer to it.
         * but not session!
         */
        session = 0;
        QPID_LOG(debug, ": async completion callback scheduled for msg seq=" << id);
        completerContext->scheduleMsgCompletion(id, requiresAccept, requiresSync);
    } else {
        // this path runs directly from the ac->end() call in handleContent() above,
        // so *session is definately valid.
        if (session->isAttached()) {
            QPID_LOG(debug, ": receive completed for msg seq=" << id);
            session->completeRcvMsg(id, requiresAccept, requiresSync);
        }
    }
    completerContext = boost::intrusive_ptr<AsyncCommandCompleter>();
}


/** Scheduled from an asynchronous command's completed callback to run on
 * the IO thread.
 */
void SessionState::AsyncCommandCompleter::schedule(boost::intrusive_ptr<AsyncCommandCompleter> ctxt)
{
    ctxt->completeCommands();
}


/** Track an ingress message that is pending completion */
void SessionState::AsyncCommandCompleter::addPendingMessage(boost::intrusive_ptr<qpid::broker::amqp_0_10::MessageTransfer> msg)
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);
    std::pair<SequenceNumber, boost::intrusive_ptr<qpid::broker::amqp_0_10::MessageTransfer> > item(msg->getCommandId(), msg);
    bool unique = pendingMsgs.insert(item).second;
    if (!unique) {
      assert(false);
    }
}


/** pending message has completed */
void SessionState::AsyncCommandCompleter::deletePendingMessage(SequenceNumber id)
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);
    pendingMsgs.erase(id);
}


/** done when an execution.sync arrives */
void SessionState::AsyncCommandCompleter::flushPendingMessages()
{
    std::map<SequenceNumber, boost::intrusive_ptr<qpid::broker::amqp_0_10::MessageTransfer> > copy;
    {
        qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);
        pendingMsgs.swap(copy);    // we've only tracked these in case a flush is needed, so nuke 'em now.
    }
    // drop lock, so it is safe to call "flush()"
    for (std::map<SequenceNumber, boost::intrusive_ptr<qpid::broker::amqp_0_10::MessageTransfer> >::iterator i = copy.begin();
         i != copy.end(); ++i) {
        i->second->flush();
    }
}


/** mark an ingress Message.Transfer command as completed.
 * This method must be thread safe - it may run on any thread.
 */
void SessionState::AsyncCommandCompleter::scheduleMsgCompletion(SequenceNumber cmd,
                                                                bool requiresAccept,
                                                                bool requiresSync)
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);

    if (session && isAttached) {
        MessageInfo msg(cmd, requiresAccept, requiresSync);
        completedMsgs.push_back(msg);
        if (completedMsgs.size() == 1) {
            session->getConnection().requestIOProcessing(boost::bind(&schedule,
                                                                     session->asyncCommandCompleter));
        }
    }
}


/** Cause the session to complete all completed commands.
 * Executes on the IO thread.
 */
void SessionState::AsyncCommandCompleter::completeCommands()
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);

    // when session is destroyed, it clears the session pointer via cancel().
    if (session && session->isAttached()) {
        for (std::vector<MessageInfo>::iterator msg = completedMsgs.begin();
             msg != completedMsgs.end(); ++msg) {
            session->completeRcvMsg(msg->cmd, msg->requiresAccept, msg->requiresSync);
        }
    }
    completedMsgs.clear();
}


/** cancel any pending calls to scheduleComplete */
void SessionState::AsyncCommandCompleter::cancel()
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);
    session = 0;
}


/** inform the completer that the session has attached,
 * allows command completion scheduling from any thread */
void SessionState::AsyncCommandCompleter::attached()
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);
    isAttached = true;
}


/** inform the completer that the session has detached,
 * disables command completion scheduling from any thread */
void SessionState::AsyncCommandCompleter::detached()
{
    qpid::sys::ScopedLock<qpid::sys::Mutex> l(completerLock);
    isAttached = false;
}

}} // namespace qpid::broker
