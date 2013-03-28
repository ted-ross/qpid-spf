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
#include <sstream>
#include <deque>
#include "unit_test.h"
#include "test_tools.h"

#include "qpid/broker/QueueFlowLimit.h"
#include "qpid/broker/QueueSettings.h"
#include "qpid/sys/Time.h"
#include "qpid/framing/reply_exceptions.h"
#include "qpid/framing/FieldValue.h"
#include "MessageUtils.h"
#include "BrokerFixture.h"

using namespace qpid::broker;
using namespace qpid::framing;

namespace qpid {
namespace tests {

QPID_AUTO_TEST_SUITE(QueueFlowLimitTestSuite)

namespace {

class TestFlow : public QueueFlowLimit
{
public:
    TestFlow(uint32_t flowStopCount, uint32_t flowResumeCount,
             uint64_t flowStopSize, uint64_t flowResumeSize) :
        QueueFlowLimit(0, flowStopCount, flowResumeCount, flowStopSize, flowResumeSize)
    {}
    virtual ~TestFlow() {}

    static TestFlow *createTestFlow(const qpid::framing::FieldTable& settings)
    {
        FieldTable::ValuePtr v;

        v = settings.get(flowStopCountKey);
        uint32_t flowStopCount = (v) ? (uint32_t)v->get<int64_t>() : 0;
        v = settings.get(flowResumeCountKey);
        uint32_t flowResumeCount = (v) ? (uint32_t)v->get<int64_t>() : 0;
        v = settings.get(flowStopSizeKey);
        uint64_t flowStopSize = (v) ? (uint64_t)v->get<int64_t>() : 0;
        v = settings.get(flowResumeSizeKey);
        uint64_t flowResumeSize = (v) ? (uint64_t)v->get<int64_t>() : 0;

        return new TestFlow(flowStopCount, flowResumeCount, flowStopSize, flowResumeSize);
    }

    static QueueFlowLimit *getQueueFlowLimit(const qpid::framing::FieldTable& arguments)
    {
        QueueSettings settings;
        settings.populate(arguments, settings.storeSettings);
        return QueueFlowLimit::createLimit(0, settings);
    }
};

Message createMessage(uint32_t size)
{
    static uint32_t seqNum;
    Message msg = MessageUtils::createMessage(qpid::types::Variant::Map(), std::string (size, 'x'));
    msg.setSequence(++seqNum);
    return msg;
}
}

QPID_AUTO_TEST_CASE(testFlowCount)
{
    FieldTable args;
    args.setInt(QueueFlowLimit::flowStopCountKey, 7);
    args.setInt(QueueFlowLimit::flowResumeCountKey, 5);

    std::auto_ptr<TestFlow> flow(TestFlow::createTestFlow(args));

    BOOST_CHECK_EQUAL((uint32_t) 7, flow->getFlowStopCount());
    BOOST_CHECK_EQUAL((uint32_t) 5, flow->getFlowResumeCount());
    BOOST_CHECK_EQUAL((uint32_t) 0, flow->getFlowStopSize());
    BOOST_CHECK_EQUAL((uint32_t) 0, flow->getFlowResumeSize());
    BOOST_CHECK(!flow->isFlowControlActive());
    BOOST_CHECK(flow->monitorFlowControl());

    std::deque<Message> msgs;
    for (size_t i = 0; i < 6; i++) {
        msgs.push_back(createMessage(10));
        flow->enqueued(msgs.back());
        BOOST_CHECK(!flow->isFlowControlActive());
    }
    BOOST_CHECK(!flow->isFlowControlActive());  // 6 on queue
    msgs.push_back(createMessage(10));
    flow->enqueued(msgs.back());
    BOOST_CHECK(!flow->isFlowControlActive());  // 7 on queue
    msgs.push_back(createMessage(10));
    flow->enqueued(msgs.back());
    BOOST_CHECK(flow->isFlowControlActive());   // 8 on queue, ON
    msgs.push_back(createMessage(10));
    flow->enqueued(msgs.back());
    BOOST_CHECK(flow->isFlowControlActive());   // 9 on queue, no change to flow control

    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 8 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 7 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 6 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 5 on queue, no change

    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(!flow->isFlowControlActive());  // 4 on queue, OFF
}

QPID_AUTO_TEST_CASE(testFlowSize)
{
    FieldTable args;
    args.setUInt64(QueueFlowLimit::flowStopSizeKey, 70);
    args.setUInt64(QueueFlowLimit::flowResumeSizeKey, 50);

    std::auto_ptr<TestFlow> flow(TestFlow::createTestFlow(args));

    BOOST_CHECK_EQUAL((uint32_t) 0, flow->getFlowStopCount());
    BOOST_CHECK_EQUAL((uint32_t) 0, flow->getFlowResumeCount());
    BOOST_CHECK_EQUAL((uint32_t) 70, flow->getFlowStopSize());
    BOOST_CHECK_EQUAL((uint32_t) 50, flow->getFlowResumeSize());
    BOOST_CHECK(!flow->isFlowControlActive());
    BOOST_CHECK(flow->monitorFlowControl());

    std::deque<Message> msgs;
    for (size_t i = 0; i < 6; i++) {
        msgs.push_back(createMessage(10));
        flow->enqueued(msgs.back());
        BOOST_CHECK(!flow->isFlowControlActive());
    }
    BOOST_CHECK(!flow->isFlowControlActive());  // 60 on queue
    BOOST_CHECK_EQUAL(6u, flow->getFlowCount());
    BOOST_CHECK_EQUAL(60u, flow->getFlowSize());

    Message msg_9 = createMessage(9);
    flow->enqueued(msg_9);
    BOOST_CHECK(!flow->isFlowControlActive());  // 69 on queue
    Message tinyMsg_1 = createMessage(1);
    flow->enqueued(tinyMsg_1);
    BOOST_CHECK(!flow->isFlowControlActive());   // 70 on queue

    Message tinyMsg_2 = createMessage(1);
    flow->enqueued(tinyMsg_2);
    BOOST_CHECK(flow->isFlowControlActive());   // 71 on queue, ON
    msgs.push_back(createMessage(10));
    flow->enqueued(msgs.back());
    BOOST_CHECK(flow->isFlowControlActive());   // 81 on queue
    BOOST_CHECK_EQUAL(10u, flow->getFlowCount());
    BOOST_CHECK_EQUAL(81u, flow->getFlowSize());

    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 71 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 61 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // 51 on queue

    flow->dequeued(tinyMsg_1);
    BOOST_CHECK(flow->isFlowControlActive());   // 50 on queue
    flow->dequeued(tinyMsg_2);
    BOOST_CHECK(!flow->isFlowControlActive());   // 49 on queue, OFF

    flow->dequeued(msg_9);
    BOOST_CHECK(!flow->isFlowControlActive());  // 40 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(!flow->isFlowControlActive());  // 30 on queue
    flow->dequeued(msgs.front());
    msgs.pop_front();
    BOOST_CHECK(!flow->isFlowControlActive());  // 20 on queue
    BOOST_CHECK_EQUAL(2u, flow->getFlowCount());
    BOOST_CHECK_EQUAL(20u, flow->getFlowSize());
}

QPID_AUTO_TEST_CASE(testFlowArgs)
{
    FieldTable args;
    const uint64_t stop(0x2FFFFFFFFull);
    const uint64_t resume(0x1FFFFFFFFull);
    args.setInt(QueueFlowLimit::flowStopCountKey, 30);
    args.setInt(QueueFlowLimit::flowResumeCountKey, 21);
    args.setUInt64(QueueFlowLimit::flowStopSizeKey, stop);
    args.setUInt64(QueueFlowLimit::flowResumeSizeKey, resume);

    std::auto_ptr<TestFlow> flow(TestFlow::createTestFlow(args));

    BOOST_CHECK_EQUAL((uint32_t) 30, flow->getFlowStopCount());
    BOOST_CHECK_EQUAL((uint32_t) 21, flow->getFlowResumeCount());
    BOOST_CHECK_EQUAL(stop, flow->getFlowStopSize());
    BOOST_CHECK_EQUAL(resume, flow->getFlowResumeSize());
    BOOST_CHECK(!flow->isFlowControlActive());
    BOOST_CHECK(flow->monitorFlowControl());
}


QPID_AUTO_TEST_CASE(testFlowCombo)
{
    FieldTable args;
    args.setInt(QueueFlowLimit::flowStopCountKey, 10);
    args.setInt(QueueFlowLimit::flowResumeCountKey, 5);
    args.setUInt64(QueueFlowLimit::flowStopSizeKey, 200);
    args.setUInt64(QueueFlowLimit::flowResumeSizeKey, 100);

    std::deque<Message> msgs_1;
    std::deque<Message> msgs_10;
    std::deque<Message> msgs_50;
    std::deque<Message> msgs_100;

    Message msg;

    std::auto_ptr<TestFlow> flow(TestFlow::createTestFlow(args));
    BOOST_CHECK(!flow->isFlowControlActive());        // count:0  size:0

    // verify flow control comes ON when only count passes its stop point.

    for (size_t i = 0; i < 10; i++) {
        msgs_10.push_back(createMessage(10));
        flow->enqueued(msgs_10.back());
        BOOST_CHECK(!flow->isFlowControlActive());
    }
    // count:10 size:100

    msgs_1.push_back(createMessage(1));
    flow->enqueued(msgs_1.back());  // count:11 size: 101  ->ON
    BOOST_CHECK(flow->isFlowControlActive());

    for (size_t i = 0; i < 6; i++) {
        flow->dequeued(msgs_10.front());
        msgs_10.pop_front();
        BOOST_CHECK(flow->isFlowControlActive());
    }
    // count:5 size: 41

    flow->dequeued(msgs_1.front());        // count: 4 size: 40  ->OFF
    msgs_1.pop_front();
    BOOST_CHECK(!flow->isFlowControlActive());

    for (size_t i = 0; i < 4; i++) {
        flow->dequeued(msgs_10.front());
        msgs_10.pop_front();
        BOOST_CHECK(!flow->isFlowControlActive());
    }
    // count:0 size:0

    // verify flow control comes ON when only size passes its stop point.

    msgs_100.push_back(createMessage(100));
    flow->enqueued(msgs_100.back());  // count:1 size: 100
    BOOST_CHECK(!flow->isFlowControlActive());

    msgs_50.push_back(createMessage(50));
    flow->enqueued(msgs_50.back());   // count:2 size: 150
    BOOST_CHECK(!flow->isFlowControlActive());

    msgs_50.push_back(createMessage(50));
    flow->enqueued(msgs_50.back());   // count:3 size: 200
    BOOST_CHECK(!flow->isFlowControlActive());

    msgs_1.push_back(createMessage(1));
    flow->enqueued(msgs_1.back());   // count:4 size: 201  ->ON
    BOOST_CHECK(flow->isFlowControlActive());

    flow->dequeued(msgs_100.front());              // count:3 size:101
    msgs_100.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());

    flow->dequeued(msgs_1.front());                // count:2 size:100
    msgs_1.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());

    flow->dequeued(msgs_50.front());               // count:1 size:50  ->OFF
    msgs_50.pop_front();
    BOOST_CHECK(!flow->isFlowControlActive());

    // verify flow control remains ON until both thresholds drop below their
    // resume point.

    for (size_t i = 0; i < 8; i++) {
        msgs_10.push_back(createMessage(10));
        flow->enqueued(msgs_10.back());
        BOOST_CHECK(!flow->isFlowControlActive());
    }
    // count:9 size:130

    msgs_10.push_back(createMessage(10));
    flow->enqueued(msgs_10.back());              // count:10 size: 140
    BOOST_CHECK(!flow->isFlowControlActive());

    msgs_1.push_back(createMessage(1));
    flow->enqueued(msgs_1.back());               // count:11 size: 141  ->ON
    BOOST_CHECK(flow->isFlowControlActive());

    msgs_100.push_back(createMessage(100));
    flow->enqueued(msgs_100.back());     // count:12 size: 241  (both thresholds crossed)
    BOOST_CHECK(flow->isFlowControlActive());

    // at this point: 9@10 + 1@50 + 1@100 + 1@1 == 12@241

    flow->dequeued(msgs_50.front());               // count:11 size:191
    msgs_50.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());

    for (size_t i = 0; i < 9; i++) {
        flow->dequeued(msgs_10.front());
        msgs_10.pop_front();
        BOOST_CHECK(flow->isFlowControlActive());
    }
    // count:2 size:101
    flow->dequeued(msgs_1.front());                // count:1 size:100
    msgs_1.pop_front();
    BOOST_CHECK(flow->isFlowControlActive());   // still active due to size

    flow->dequeued(msgs_100.front());               // count:0 size:0  ->OFF
    msgs_100.pop_front();
    BOOST_CHECK(!flow->isFlowControlActive());
}


QPID_AUTO_TEST_CASE(testFlowDefaultArgs)
{
    QueueFlowLimit::setDefaults(2950001, // max queue byte count
                                80,     // 80% stop threshold
                                70);    // 70% resume threshold
    FieldTable args;
    QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);

    BOOST_CHECK(ptr);
    std::auto_ptr<QueueFlowLimit> flow(ptr);
    BOOST_CHECK_EQUAL((uint64_t) 2360001, flow->getFlowStopSize());
    BOOST_CHECK_EQUAL((uint64_t) 2065000, flow->getFlowResumeSize());
    BOOST_CHECK_EQUAL( 0u, flow->getFlowStopCount());
    BOOST_CHECK_EQUAL( 0u, flow->getFlowResumeCount());
    BOOST_CHECK(!flow->isFlowControlActive());
    BOOST_CHECK(flow->monitorFlowControl());
}


QPID_AUTO_TEST_CASE(testFlowOverrideArgs)
{
    QueueFlowLimit::setDefaults(2950001, // max queue byte count
                                80,     // 80% stop threshold
                                70);    // 70% resume threshold
    {
        FieldTable args;
        args.setInt(QueueFlowLimit::flowStopCountKey, 35000);
        args.setInt(QueueFlowLimit::flowResumeCountKey, 30000);

        QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);
        BOOST_CHECK(ptr);
        std::auto_ptr<QueueFlowLimit> flow(ptr);

        BOOST_CHECK_EQUAL((uint32_t) 35000, flow->getFlowStopCount());
        BOOST_CHECK_EQUAL((uint32_t) 30000, flow->getFlowResumeCount());
        BOOST_CHECK_EQUAL((uint64_t) 0, flow->getFlowStopSize());
        BOOST_CHECK_EQUAL((uint64_t) 0, flow->getFlowResumeSize());
        BOOST_CHECK(!flow->isFlowControlActive());
        BOOST_CHECK(flow->monitorFlowControl());
    }
    {
        FieldTable args;
        args.setInt(QueueFlowLimit::flowStopSizeKey, 350000);
        args.setInt(QueueFlowLimit::flowResumeSizeKey, 300000);

        QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);
        BOOST_CHECK(ptr);
        std::auto_ptr<QueueFlowLimit> flow(ptr);

        BOOST_CHECK_EQUAL((uint32_t) 0, flow->getFlowStopCount());
        BOOST_CHECK_EQUAL((uint32_t) 0, flow->getFlowResumeCount());
        BOOST_CHECK_EQUAL((uint64_t) 350000, flow->getFlowStopSize());
        BOOST_CHECK_EQUAL((uint64_t) 300000, flow->getFlowResumeSize());
        BOOST_CHECK(!flow->isFlowControlActive());
        BOOST_CHECK(flow->monitorFlowControl());
    }
    {
        FieldTable args;
        args.setInt(QueueFlowLimit::flowStopCountKey, 35000);
        args.setInt(QueueFlowLimit::flowResumeCountKey, 30000);
        args.setInt(QueueFlowLimit::flowStopSizeKey, 350000);
        args.setInt(QueueFlowLimit::flowResumeSizeKey, 300000);

        QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);
        BOOST_CHECK(ptr);
        std::auto_ptr<QueueFlowLimit> flow(ptr);

        BOOST_CHECK_EQUAL((uint32_t) 35000, flow->getFlowStopCount());
        BOOST_CHECK_EQUAL((uint32_t) 30000, flow->getFlowResumeCount());
        BOOST_CHECK_EQUAL((uint64_t) 350000, flow->getFlowStopSize());
        BOOST_CHECK_EQUAL((uint64_t) 300000, flow->getFlowResumeSize());
        BOOST_CHECK(!flow->isFlowControlActive());
        BOOST_CHECK(flow->monitorFlowControl());
    }
}


QPID_AUTO_TEST_CASE(testFlowOverrideDefaults)
{
    QueueFlowLimit::setDefaults(2950001, // max queue byte count
                                97,     // stop threshold
                                73);    // resume threshold
    FieldTable args;
    QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);
    BOOST_CHECK(ptr);
    std::auto_ptr<QueueFlowLimit> flow(ptr);

    BOOST_CHECK_EQUAL((uint32_t) 2861501, flow->getFlowStopSize());
    BOOST_CHECK_EQUAL((uint32_t) 2153500, flow->getFlowResumeSize());
    BOOST_CHECK(!flow->isFlowControlActive());
    BOOST_CHECK(flow->monitorFlowControl());
}


QPID_AUTO_TEST_CASE(testFlowDisable)
{
    {
        FieldTable args;
        args.setInt(QueueFlowLimit::flowStopCountKey, 0);
        QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);
        BOOST_CHECK(!ptr);
    }
    {
        FieldTable args;
        args.setInt(QueueFlowLimit::flowStopSizeKey, 0);
        QueueFlowLimit *ptr = TestFlow::getQueueFlowLimit(args);
        BOOST_CHECK(!ptr);
    }
}

QPID_AUTO_TEST_SUITE_END()

}} // namespace qpid::tests
