#ifndef QPID_BROKER_AMQP_NODEPROPERTIES_H
#define QPID_BROKER_AMQP_NODEPROPERTIES_H

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
#include "qpid/amqp/MapReader.h"
#include "qpid/types/Variant.h"

struct pn_data_t;
namespace qpid {
namespace broker {
struct QueueSettings;
namespace amqp {

class NodeProperties : public qpid::amqp::MapReader
{
  public:
    NodeProperties();
    void read(pn_data_t*);
    void onNullValue(const qpid::amqp::CharSequence&, const qpid::amqp::Descriptor*);
    void onBooleanValue(const qpid::amqp::CharSequence&, bool, const qpid::amqp::Descriptor*);
    void onUByteValue(const qpid::amqp::CharSequence&, uint8_t, const qpid::amqp::Descriptor*);
    void onUShortValue(const qpid::amqp::CharSequence&, uint16_t, const qpid::amqp::Descriptor*);
    void onUIntValue(const qpid::amqp::CharSequence&, uint32_t, const qpid::amqp::Descriptor*);
    void onULongValue(const qpid::amqp::CharSequence&, uint64_t, const qpid::amqp::Descriptor*);
    void onByteValue(const qpid::amqp::CharSequence&, int8_t, const qpid::amqp::Descriptor*);
    void onShortValue(const qpid::amqp::CharSequence&, int16_t, const qpid::amqp::Descriptor*);
    void onIntValue(const qpid::amqp::CharSequence&, int32_t, const qpid::amqp::Descriptor*);
    void onLongValue(const qpid::amqp::CharSequence&, int64_t, const qpid::amqp::Descriptor*);
    void onFloatValue(const qpid::amqp::CharSequence&, float, const qpid::amqp::Descriptor*);
    void onDoubleValue(const qpid::amqp::CharSequence&, double, const qpid::amqp::Descriptor*);
    void onUuidValue(const qpid::amqp::CharSequence&, const qpid::amqp::CharSequence&, const qpid::amqp::Descriptor*);
    void onTimestampValue(const qpid::amqp::CharSequence&, int64_t, const qpid::amqp::Descriptor*);
    void onStringValue(const qpid::amqp::CharSequence&, const qpid::amqp::CharSequence&, const qpid::amqp::Descriptor*);
    void onSymbolValue(const qpid::amqp::CharSequence&, const qpid::amqp::CharSequence&, const qpid::amqp::Descriptor*);
    bool isQueue() const;
    QueueSettings getQueueSettings();
    bool isDurable() const;
    std::string getExchangeType() const;
    std::string getAlternateExchange() const;
  private:
    bool queue;
    bool durable;
    bool autoDelete;
    std::string exchangeType;
    std::string alternateExchange;
    qpid::types::Variant::Map properties;

    void process(const std::string&, const qpid::types::Variant&);
};
}}} // namespace qpid::broker::amqp

#endif  /*!QPID_BROKER_AMQP_NODEPROPERTIES_H*/
