﻿/*
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

namespace Org.Apache.Qpid.Messaging.UnitTest
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using Org.Apache.Qpid.Messaging;
    using NUnit.Framework;

    [TestFixture]
    public class MessageTests
    {
        [SetUp]
        public void SetUp()
        {
        }

        [TearDown]
        public void TearDown()
        {
        }

        [Test]
        public void SimpleMessageSize()
        {
            Message m2 = new Message("rarey");
            UInt64 m2Size = m2.ContentSize;
            Assert.AreEqual(5, m2Size);
        }

        [Test]
        public void SimpleMessageStringContent()
        {
            Message m2 = new Message("rarely");
            string mString = m2.GetContent();

            StringAssert.IsMatch("rarely", mString);
        }

        [Test]
        public void MessageReceiveContentAsByteArray()
        {
            Message m2 = new Message("while");
            UInt64 m2Size = m2.ContentSize;

            byte[] myRaw = new byte [m2Size];

            m2.GetContent(myRaw);

            Assert.IsTrue(true);
        }

        [Test]
        public void MessageAsByteArray()
        {
            byte[] rawData = new byte[10];
            for (byte i = 0; i < 10; i++)
                rawData[i] = i;
            Message m3 = new Message(rawData);

            byte[] readback = new byte[m3.ContentSize];
            m3.GetContent(readback);

            for (byte i = 0; i < 10; i++)
                Assert.AreEqual(i, readback[i]);
        }

        [Test]
        public void MessageAsByteArraySlice()
        {
            byte[] rawData = new byte[10];
            for (byte i = 0; i < 10; i++)
                rawData[i] = i;
            Message m3 = new Message(rawData, 1, 8);

            Assert.AreEqual(8, m3.ContentSize);

            byte[] readback = new byte[m3.ContentSize];
            m3.GetContent(readback);

            for (byte i = 0; i < 8; i++)
                Assert.AreEqual(i + 1, readback[i]);
        }


        [Test]
        public void MessageProperties()
        {
            Message msgGetSet = new Message("12345");

            msgGetSet.Subject = "Subject";
            msgGetSet.MessageId = "MessageId";
            msgGetSet.UserId = "UserId";
            msgGetSet.CorrelationId = "CorrelationId";
            msgGetSet.Ttl = DurationConstants.SECOND;
            msgGetSet.Priority = (byte)'z';
            msgGetSet.Durable = false;
            msgGetSet.Redelivered = true;

            Dictionary<string, object> props = new Dictionary<string,object>();
            props.Add("firstProperty", 1);
            props.Add("secondProperty", 2);
            msgGetSet.Properties = props;

            Address replyToAddr = new Address("replyTo");
            replyToAddr.Subject = "topsecret";
            msgGetSet.ReplyTo = replyToAddr;

            StringAssert.IsMatch("Subject",       msgGetSet.Subject);
            StringAssert.IsMatch("",              msgGetSet.ContentType);
            StringAssert.IsMatch("MessageId",     msgGetSet.MessageId);
            StringAssert.IsMatch("UserId",        msgGetSet.UserId);
            StringAssert.IsMatch("CorrelationId", msgGetSet.CorrelationId);
            Assert.AreEqual(1000,                 msgGetSet.Ttl.Milliseconds);
            Assert.AreEqual((byte)'z',            msgGetSet.Priority);
            Assert.IsFalse(                       msgGetSet.Durable);
            Assert.IsTrue(                        msgGetSet.Redelivered);

            Dictionary<string, object> gotProps = msgGetSet.Properties;
            StringAssert.IsMatch("1", gotProps["firstProperty"].ToString());
            StringAssert.IsMatch("2", gotProps["secondProperty"].ToString());

            Address gotReply = msgGetSet.ReplyTo;
            StringAssert.IsMatch("replyTo", gotReply.Name);
            StringAssert.IsMatch("topsecret", msgGetSet.ReplyTo.Subject);
        }

        [Test]
        public void SimpleMessageCopy()
        {
            Message m2 = new Message("rarely");
            Message m3 = m2;

            StringAssert.IsMatch("rarely", m3.GetContent());
        }

        [Test]
        public void MessageAsMap_AllVariableTypes()
        {
            //
            // Create structured content for the message.  This example builds a
            // map of items including a nested map and a list of values.
            //
            Dictionary<string, object> content = new Dictionary<string, object>();
            Dictionary<string, object> subMap = new Dictionary<string, object>();
            Collection<object> colors = new Collection<object>();

            // add simple types
            content["id"] = 987654321;
            content["name"] = "Widget";
            content["percent"] = 0.99;

            // add nested amqp/map
            subMap["name"] = "Smith";
            subMap["number"] = 354;
            content["nestedMap"] = subMap;

            // add an amqp/list
            colors.Add("red");
            colors.Add("green");
            colors.Add("white");
            // list contains null value
            colors.Add(null);
            content["colorsList"] = colors;

            // add one of each supported amqp data type
            bool mybool = true;
            content["mybool"] = mybool;

            byte mybyte = 4;
            content["mybyte"] = mybyte;

            UInt16 myUInt16 = 5;
            content["myUInt16"] = myUInt16;

            UInt32 myUInt32 = 6;
            content["myUInt32"] = myUInt32;

            UInt64 myUInt64 = 7;
            content["myUInt64"] = myUInt64;

            char mychar = 'h';
            content["mychar"] = mychar;

            Int16 myInt16 = 9;
            content["myInt16"] = myInt16;

            Int32 myInt32 = 10;
            content["myInt32"] = myInt32;

            Int64 myInt64 = 11;
            content["myInt64"] = myInt64;

            Single mySingle = (Single)12.12;
            content["mySingle"] = mySingle;

            Double myDouble = 13.13;
            content["myDouble"] = myDouble;

            Guid myGuid = new Guid("000102030405060708090a0b0c0d0e0f");
            content["myGuid"] = myGuid;

            content["myNull"] = null;

            // Create the message
            Message message = new Message(content);

            // Copy the message
            Message rxMsg = message;

            // Extract the content
            Dictionary<string, object> rxContent = new Dictionary<string, object>();

            rxMsg.GetContent(rxContent);

            Dictionary<string, object> rxSubMap = (Dictionary<string, object>)rxContent["nestedMap"];

            Collection<object> rxColors = (Collection<object>)rxContent["colorsList"];

            StringAssert.IsMatch("System.Boolean", rxContent["mybool"].GetType().ToString());
            bool rxbool = (bool)rxContent["mybool"];

            StringAssert.IsMatch("System.SByte", rxContent["mybyte"].GetType().ToString());
            sbyte rxbyte = (sbyte)rxContent["mybyte"];

            StringAssert.IsMatch("System.UInt16", rxContent["myUInt16"].GetType().ToString());
            UInt16 rxUInt16 = (UInt16)rxContent["myUInt16"];

            StringAssert.IsMatch("System.UInt32", rxContent["myUInt32"].GetType().ToString());
            UInt32 rxUInt32 = (UInt32)rxContent["myUInt32"];

            StringAssert.IsMatch("System.UInt64", rxContent["myUInt64"].GetType().ToString());
            UInt64 rxUInt64 = (UInt64)rxContent["myUInt64"];

            StringAssert.IsMatch("System.Int32", rxContent["mychar"].GetType().ToString());
            char rxchar = System.Convert.ToChar(rxContent["mychar"]);

            StringAssert.IsMatch("System.Int16", rxContent["myInt16"].GetType().ToString());
            Int16 rxInt16 = (Int16)rxContent["myInt16"];

            StringAssert.IsMatch("System.Int32", rxContent["myInt32"].GetType().ToString());
            Int32 rxInt32 = (Int32)rxContent["myInt32"];

            StringAssert.IsMatch("System.Int64", rxContent["myInt64"].GetType().ToString());
            Int64 rxInt64 = (Int64)rxContent["myInt64"];

            StringAssert.IsMatch("System.Single", rxContent["mySingle"].GetType().ToString());
            Single rxSingle = (Single)rxContent["mySingle"];

            StringAssert.IsMatch("System.Double", rxContent["myDouble"].GetType().ToString());
            Double rxDouble = (Double)rxContent["myDouble"];

            StringAssert.IsMatch("System.Guid", rxContent["myGuid"].GetType().ToString());
            Guid rxGuid = (Guid)rxContent["myGuid"];

            // Verify the values

            StringAssert.IsMatch("Smith", rxSubMap["name"].ToString());
            Assert.AreEqual(4, rxColors.Count);
            Assert.IsTrue(rxbool);
            Assert.AreEqual(4, rxbyte);
            Assert.AreEqual(5, rxUInt16);
            Assert.AreEqual(6, rxUInt32);
            Assert.AreEqual(7, rxUInt64);
            Assert.AreEqual((char)'h', rxchar);
            Assert.AreEqual(9, rxInt16);
            Assert.AreEqual(10, rxInt32);
            Assert.AreEqual(11, rxInt64);
            Assert.AreEqual((Single)12.12, rxSingle);
            Assert.AreEqual((Double)13.13, rxDouble);
            StringAssert.IsMatch("00010203-0405-0607-0809-0a0b0c0d0e0f", rxGuid.ToString());
        }
    }
}
