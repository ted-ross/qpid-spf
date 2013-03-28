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
#include "qpid/messaging/amqp/EncodedMessage.h"
#include "qpid/messaging/Address.h"
#include "qpid/messaging/MessageImpl.h"
#include "qpid/amqp/Decoder.h"
#include <boost/lexical_cast.hpp>
#include <string.h>

namespace qpid {
namespace messaging {
namespace amqp {

using namespace qpid::amqp;

EncodedMessage::EncodedMessage(size_t s) : size(s), data(size ? new char[size] : 0)
{
    init();
}

EncodedMessage::EncodedMessage() : size(0), data(0)
{
    init();
}

EncodedMessage::EncodedMessage(const EncodedMessage& other) : size(other.size), data(size ? new char[size] : 0)
{
    init();
}

void EncodedMessage::init()
{
    //init all CharSequence members
    deliveryAnnotations.init();
    messageAnnotations.init();
    userId.init();
    to.init();
    subject.init();
    replyTo.init();
    contentType.init();
    contentEncoding.init();
    groupId.init();
    replyToGroupId.init();
    applicationProperties.init();
    body.init();
    footer.init();
}

EncodedMessage::~EncodedMessage()
{
    delete[] data;
}

size_t EncodedMessage::getSize() const
{
    return size;
}
void EncodedMessage::trim(size_t t)
{
    size = t;
}
void EncodedMessage::resize(size_t s)
{
    delete[] data;
    size = s;
    data = new char[size];
}

char* EncodedMessage::getData()
{
    return data;
}
const char* EncodedMessage::getData() const
{
    return data;
}

void EncodedMessage::init(qpid::messaging::MessageImpl& impl)
{
    //initial scan of raw data
    qpid::amqp::Decoder decoder(data, size);
    InitialScan reader(*this, impl);
    decoder.read(reader);
    bareMessage = reader.getBareMessage();
    if (bareMessage.data && !bareMessage.size) {
        bareMessage.size = (data + size) - bareMessage.data;
    }

}
void EncodedMessage::populate(qpid::types::Variant::Map& map) const
{
    //decode application properties
    if (applicationProperties) {
        qpid::amqp::Decoder decoder(applicationProperties.data, applicationProperties.size);
        decoder.readMap(map);
    }
    //add in 'x-amqp-' prefixed values
    if (!!firstAcquirer) {
        map["x-amqp-first-acquirer"] = firstAcquirer.get();
    }
    if (!!deliveryCount) {
        map["x-amqp-delivery-count"] = deliveryCount.get();
    }
    if (to) {
        map["x-amqp-delivery-count"] = to.str();
    }
    if (!!absoluteExpiryTime) {
        map["x-amqp-absolute-expiry-time"] = absoluteExpiryTime.get();
    }
    if (!!creationTime) {
        map["x-amqp-creation-time"] = creationTime.get();
    }
    if (groupId) {
        map["x-amqp-group-id"] = groupId.str();
    }
    if (!!groupSequence) {
        map["x-amqp-qroup-sequence"] = groupSequence.get();
    }
    if (replyToGroupId) {
        map["x-amqp-reply-to-group-id"] = replyToGroupId.str();
    }
    //add in any annotations
    if (deliveryAnnotations) {
        qpid::types::Variant::Map& annotations = map["x-amqp-delivery-annotations"].asMap();
        qpid::amqp::Decoder decoder(deliveryAnnotations.data, deliveryAnnotations.size);
        decoder.readMap(annotations);
    }
    if (messageAnnotations) {
        qpid::types::Variant::Map& annotations = map["x-amqp-message-annotations"].asMap();
        qpid::amqp::Decoder decoder(messageAnnotations.data, messageAnnotations.size);
        decoder.readMap(annotations);
    }
}
qpid::amqp::CharSequence EncodedMessage::getBareMessage() const
{
    return bareMessage;
}

void EncodedMessage::getReplyTo(qpid::messaging::Address& a) const
{
    a = qpid::messaging::Address(replyTo.str());
}
void EncodedMessage::getSubject(std::string& s) const
{
    s.assign(subject.data, subject.size);
}
void EncodedMessage::getContentType(std::string& s) const
{
    s.assign(contentType.data, contentType.size);
}
void EncodedMessage::getUserId(std::string& s) const
{
    s.assign(userId.data, userId.size);
}
void EncodedMessage::getMessageId(std::string& s) const
{
    messageId.assign(s);
}
void EncodedMessage::getCorrelationId(std::string& s) const
{
    correlationId.assign(s);
}
void EncodedMessage::getBody(std::string& s) const
{
    s.assign(body.data, body.size);
}

qpid::amqp::CharSequence EncodedMessage::getBody() const
{
    return body;
}

bool EncodedMessage::hasHeaderChanged(const qpid::messaging::MessageImpl& msg) const
{
    if (!durable) {
        if (msg.isDurable()) return true;
    } else {
        if (durable.get() != msg.isDurable()) return true;
    }

    if (!priority) {
        if (msg.getPriority() != 4) return true;
    } else {
        if (priority.get() != msg.getPriority()) return true;
    }

    if (msg.getTtl() && (!ttl || msg.getTtl() != ttl.get())) {
        return true;
    }

    //first-acquirer can't be changed via Message interface as yet

    if (msg.isRedelivered() && (!deliveryCount || deliveryCount.get() == 0)) {
        return true;
    }

    return false;
}


EncodedMessage::InitialScan::InitialScan(EncodedMessage& e, qpid::messaging::MessageImpl& m) : em(e), mi(m)
{
    //set up defaults as needed:
    mi.setPriority(4);
}
//header:
void EncodedMessage::InitialScan::onDurable(bool b) { mi.setDurable(b); em.durable = b; }
void EncodedMessage::InitialScan::onPriority(uint8_t i) { mi.setPriority(i); em.priority = i; }
void EncodedMessage::InitialScan::onTtl(uint32_t i) { mi.setTtl(i); em.ttl = i; }
void EncodedMessage::InitialScan::onFirstAcquirer(bool b) { em.firstAcquirer = b; }
void EncodedMessage::InitialScan::onDeliveryCount(uint32_t i)
{
    mi.setRedelivered(i > 1);
    em.deliveryCount = i;
}

//properties:
void EncodedMessage::InitialScan::onMessageId(uint64_t v) { em.messageId.set(v); }
void EncodedMessage::InitialScan::onMessageId(const qpid::amqp::CharSequence& v, qpid::types::VariantType t) { em.messageId.set(v, t); }
void EncodedMessage::InitialScan::onUserId(const qpid::amqp::CharSequence& v) { em.userId = v; }
void EncodedMessage::InitialScan::onTo(const qpid::amqp::CharSequence& v) { em.to = v; }
void EncodedMessage::InitialScan::onSubject(const qpid::amqp::CharSequence& v) { em.subject = v; }
void EncodedMessage::InitialScan::onReplyTo(const qpid::amqp::CharSequence& v) { em.replyTo = v;}
void EncodedMessage::InitialScan::onCorrelationId(uint64_t v) { em.correlationId.set(v); }
void EncodedMessage::InitialScan::onCorrelationId(const qpid::amqp::CharSequence& v, qpid::types::VariantType t) { em.correlationId.set(v, t); }
void EncodedMessage::InitialScan::onContentType(const qpid::amqp::CharSequence& v) { em.contentType = v; }
void EncodedMessage::InitialScan::onContentEncoding(const qpid::amqp::CharSequence& v) { em.contentEncoding = v; }
void EncodedMessage::InitialScan::onAbsoluteExpiryTime(int64_t i) { em.absoluteExpiryTime = i; }
void EncodedMessage::InitialScan::onCreationTime(int64_t i) { em.creationTime = i; }
void EncodedMessage::InitialScan::onGroupId(const qpid::amqp::CharSequence& v) { em.groupId = v; }
void EncodedMessage::InitialScan::onGroupSequence(uint32_t i) { em.groupSequence = i; }
void EncodedMessage::InitialScan::onReplyToGroupId(const qpid::amqp::CharSequence& v) { em.replyToGroupId = v; }

void EncodedMessage::InitialScan::onApplicationProperties(const qpid::amqp::CharSequence& v) { em.applicationProperties = v; }
void EncodedMessage::InitialScan::onDeliveryAnnotations(const qpid::amqp::CharSequence& v) { em.deliveryAnnotations = v; }
void EncodedMessage::InitialScan::onMessageAnnotations(const qpid::amqp::CharSequence& v) { em.messageAnnotations = v; }
void EncodedMessage::InitialScan::onBody(const qpid::amqp::CharSequence& v, const qpid::amqp::Descriptor&)
{
    //TODO: how to communicate the type, i.e. descriptor?
    em.body = v;
}
void EncodedMessage::InitialScan::onBody(const qpid::types::Variant&, const qpid::amqp::Descriptor&) {}
void EncodedMessage::InitialScan::onFooter(const qpid::amqp::CharSequence& v) { em.footer = v; }

}}} // namespace qpid::messaging::amqp
