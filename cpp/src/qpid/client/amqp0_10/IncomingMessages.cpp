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
#include "qpid/client/amqp0_10/IncomingMessages.h"
#include "qpid/client/amqp0_10/AddressResolution.h"
#include "qpid/amqp_0_10/Codecs.h"
#include "qpid/client/SessionImpl.h"
#include "qpid/client/SessionBase_0_10Access.h"
#include "qpid/log/Statement.h"
#include "qpid/messaging/Address.h"
#include "qpid/messaging/Duration.h"
#include "qpid/messaging/Message.h"
#include "qpid/messaging/MessageImpl.h"
#include "qpid/types/Variant.h"
#include "qpid/framing/DeliveryProperties.h"
#include "qpid/framing/FrameSet.h"
#include "qpid/framing/MessageProperties.h"
#include "qpid/framing/MessageTransferBody.h"
#include "qpid/framing/enum.h"

namespace qpid {
namespace client {
namespace amqp0_10 {

using namespace qpid::framing;
using namespace qpid::framing::message;
using namespace qpid::amqp_0_10;
using qpid::sys::AbsTime;
using qpid::sys::Duration;
using qpid::messaging::MessageImplAccess;
using qpid::types::Variant;

namespace {
const std::string EMPTY_STRING;


struct GetNone : IncomingMessages::Handler
{
    bool accept(IncomingMessages::MessageTransfer&) { return false; }
};

struct GetAny : IncomingMessages::Handler
{
    bool accept(IncomingMessages::MessageTransfer& transfer)
    { 
        transfer.retrieve(0);
        return true;
    }
};

struct MatchAndTrack
{
    const std::string destination;
    SequenceSet ids;

    MatchAndTrack(const std::string& d) : destination(d) {}

    bool operator()(boost::shared_ptr<qpid::framing::FrameSet> command)
    {
        if (command->as<MessageTransferBody>()->getDestination() == destination) {
            ids.add(command->getId());
            return true;
        } else {
            return false;
        }
    }
};

struct Match
{
    const std::string destination;
    uint32_t matched;

    Match(const std::string& d) : destination(d), matched(0) {}

    bool operator()(boost::shared_ptr<qpid::framing::FrameSet> command)
    {
        if (command->as<MessageTransferBody>()->getDestination() == destination) {
            ++matched;
            return true;
        } else {
            return false;
        }
    }
};
}

void IncomingMessages::setSession(qpid::client::AsyncSession s)
{
    sys::Mutex::ScopedLock l(lock);
    session = s;
    incoming = SessionBase_0_10Access(session).get()->getDemux().getDefault();
    acceptTracker.reset();
}

bool IncomingMessages::get(Handler& handler, Duration timeout)
{
    {
        sys::Mutex::ScopedLock l(lock);
        //search through received list for any transfer of interest:
        for (FrameSetQueue::iterator i = received.begin(); i != received.end(); i++)
        {
            MessageTransfer transfer(*i, *this);
            if (handler.accept(transfer)) {
                received.erase(i);
                return true;
            }
        }
    }
    //none found, check incoming:
    return process(&handler, timeout);
}

bool IncomingMessages::getNextDestination(std::string& destination, Duration timeout)
{
    sys::Mutex::ScopedLock l(lock);
    //if there is not already a received message, we must wait for one
    if (received.empty() && !wait(timeout)) return false;
    //else we have a message in received; return the corresponding destination
    destination = received.front()->as<MessageTransferBody>()->getDestination();
    return true;
}

void IncomingMessages::accept()
{
    sys::Mutex::ScopedLock l(lock);
    acceptTracker.accept(session);
}

void IncomingMessages::accept(qpid::framing::SequenceNumber id, bool cumulative)
{
    sys::Mutex::ScopedLock l(lock);
    acceptTracker.accept(id, session, cumulative);
}


void IncomingMessages::releaseAll()
{
    {
        //first process any received messages...
        sys::Mutex::ScopedLock l(lock);
        while (!received.empty()) {
            retrieve(received.front(), 0);
            received.pop_front();
        }
    }
    //then pump out any available messages from incoming queue...
    GetAny handler;
    while (process(&handler, 0)) ;
    //now release all messages
    sys::Mutex::ScopedLock l(lock);
    acceptTracker.release(session);
}

void IncomingMessages::releasePending(const std::string& destination)
{
    //first pump all available messages from incoming to received...
    while (process(0, 0)) ;

    //now remove all messages for this destination from received list, recording their ids...
    sys::Mutex::ScopedLock l(lock);
    MatchAndTrack match(destination);
    for (FrameSetQueue::iterator i = received.begin(); i != received.end(); i = match(*i) ? received.erase(i) : ++i) ;
    //now release those messages
    session.messageRelease(match.ids);
}

/**
 * Get a frameset that is accepted by the specified handler from
 * session queue, waiting for up to the specified duration and
 * returning true if this could be achieved, false otherwise. Messages
 * that are not accepted by the handler are pushed onto received queue
 * for later retrieval.
 */
bool IncomingMessages::process(Handler* handler, qpid::sys::Duration duration)
{
    AbsTime deadline(AbsTime::now(), duration);
    FrameSet::shared_ptr content;
    try {
        for (Duration timeout = duration; incoming->pop(content, timeout); timeout = Duration(AbsTime::now(), deadline)) {
            if (content->isA<MessageTransferBody>()) {
                MessageTransfer transfer(content, *this);
                if (handler && handler->accept(transfer)) {
                    QPID_LOG(debug, "Delivered " << *content->getMethod() << " "
                             << *content->getHeaders());
                    return true;
                } else {
                    //received message for another destination, keep for later
                    QPID_LOG(debug, "Pushed " << *content->getMethod() << " to received queue");
                    sys::Mutex::ScopedLock l(lock);
                    received.push_back(content);
                }
            } else {
                //TODO: handle other types of commands (e.g. message-accept, message-flow etc)
            }
        }
    }
    catch (const qpid::ClosedException&) {} // Just return false if queue closed.
    return false;
}

bool IncomingMessages::wait(qpid::sys::Duration duration)
{
    AbsTime deadline(AbsTime::now(), duration);
    FrameSet::shared_ptr content;
    for (Duration timeout = duration; incoming->pop(content, timeout); timeout = Duration(AbsTime::now(), deadline)) {
        if (content->isA<MessageTransferBody>()) {
            QPID_LOG(debug, "Pushed " << *content->getMethod() << " to received queue");
            sys::Mutex::ScopedLock l(lock);
            received.push_back(content);
            return true;
        } else {
            //TODO: handle other types of commands (e.g. message-accept, message-flow etc)
        }
    }
    return false;
}

uint32_t IncomingMessages::pendingAccept()
{
    sys::Mutex::ScopedLock l(lock);
    return acceptTracker.acceptsPending();
}
uint32_t IncomingMessages::pendingAccept(const std::string& destination)
{
    sys::Mutex::ScopedLock l(lock);
    return acceptTracker.acceptsPending(destination);
}

uint32_t IncomingMessages::available()
{
    //first pump all available messages from incoming to received...
    while (process(0, 0)) {}
    //return the count of received messages
    sys::Mutex::ScopedLock l(lock);
    return received.size();
}

uint32_t IncomingMessages::available(const std::string& destination)
{
    //first pump all available messages from incoming to received...
    while (process(0, 0)) {}

    //count all messages for this destination from received list
    sys::Mutex::ScopedLock l(lock);
    return std::for_each(received.begin(), received.end(), Match(destination)).matched;
}

void populate(qpid::messaging::Message& message, FrameSet& command);

/**
 * Called when message is retrieved; records retrieval for subsequent
 * acceptance, marks the command as completed and converts command to
 * message if message is required
 */
void IncomingMessages::retrieve(FrameSetPtr command, qpid::messaging::Message* message)
{
    if (message) {
        populate(*message, *command);
    }
    const MessageTransferBody* transfer = command->as<MessageTransferBody>(); 
    if (transfer->getAcceptMode() == ACCEPT_MODE_EXPLICIT) {
        sys::Mutex::ScopedLock l(lock);
        acceptTracker.delivered(transfer->getDestination(), command->getId());
    }
    session.markCompleted(command->getId(), false, false);
}

IncomingMessages::MessageTransfer::MessageTransfer(FrameSetPtr c, IncomingMessages& p) : content(c), parent(p) {}

const std::string& IncomingMessages::MessageTransfer::getDestination()
{
    return content->as<MessageTransferBody>()->getDestination();
}
void IncomingMessages::MessageTransfer::retrieve(qpid::messaging::Message* message)
{
    parent.retrieve(content, message);
}


namespace {
//TODO: unify conversion to and from 0-10 message that is currently
//split between IncomingMessages and OutgoingMessage
const std::string SUBJECT("qpid.subject");

const std::string X_APP_ID("x-amqp-0-10.app-id");
const std::string X_ROUTING_KEY("x-amqp-0-10.routing-key");
const std::string X_CONTENT_ENCODING("x-amqp-0-10.content-encoding");
const std::string X_TIMESTAMP("x-amqp-0-10.timestamp");
}

void populateHeaders(qpid::messaging::Message& message, 
                     const DeliveryProperties* deliveryProperties, 
                     const MessageProperties* messageProperties)
{
    if (deliveryProperties) {
        message.setTtl(qpid::messaging::Duration(deliveryProperties->getTtl()));
        message.setDurable(deliveryProperties->getDeliveryMode() == DELIVERY_MODE_PERSISTENT);
        message.setPriority(deliveryProperties->getPriority());
        message.setRedelivered(deliveryProperties->getRedelivered());
    }
    if (messageProperties) {
        message.setContentType(messageProperties->getContentType());
        if (messageProperties->hasReplyTo()) {
            message.setReplyTo(AddressResolution::convert(messageProperties->getReplyTo()));
        }
        message.setSubject(messageProperties->getApplicationHeaders().getAsString(SUBJECT));
        message.getProperties().clear();
        translate(messageProperties->getApplicationHeaders(), message.getProperties());
        message.setCorrelationId(messageProperties->getCorrelationId());
        message.setUserId(messageProperties->getUserId());
        if (messageProperties->hasMessageId()) {
            message.setMessageId(messageProperties->getMessageId().str());
        }
        //expose 0-10 specific items through special properties: 
        //    app-id, content-encoding
        if (messageProperties->hasAppId()) {
            message.getProperties()[X_APP_ID] = messageProperties->getAppId();
        }
        if (messageProperties->hasContentEncoding()) {
            message.getProperties()[X_CONTENT_ENCODING] = messageProperties->getContentEncoding();
        }
        //    routing-key, timestamp, others?
        if (deliveryProperties && deliveryProperties->hasRoutingKey()) {
            message.getProperties()[X_ROUTING_KEY] = deliveryProperties->getRoutingKey();
        }
        if (deliveryProperties && deliveryProperties->hasTimestamp()) {
            message.getProperties()[X_TIMESTAMP] = deliveryProperties->getTimestamp();
        }
    }
}

void populateHeaders(qpid::messaging::Message& message, const AMQHeaderBody* headers)
{
    populateHeaders(message, headers->get<DeliveryProperties>(), headers->get<MessageProperties>());
}

void populate(qpid::messaging::Message& message, FrameSet& command)
{
    //need to be able to link the message back to the transfer it was delivered by
    //e.g. for rejecting.
    MessageImplAccess::get(message).setInternalId(command.getId());
        
    message.setContent(command.getContent());

    populateHeaders(message, command.getHeaders());
}


}}} // namespace qpid::client::amqp0_10
