#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
from qpid.testlib import TestBase010
from qpid.datatypes import Message
from qpid.queue import Empty
from time import sleep
import qmf.console

class QmfInteropTests(TestBase010):

    def test_A_agent_presence(self):
        self.startQmf();
        qmf = self.qmf

        agents = []
        count = 0
        while len(agents) == 0:
            agents = qmf.getObjects(_class="agent")
            sleep(1)
            count += 1
            if count > 10:
                self.fail("Timed out waiting for remote agent")

    def test_B_basic_method_invocation(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]
        for seq in range(10):
            result = parent.echo(seq, _timeout=5)
            self.assertEqual(result.status, 0)
            self.assertEqual(result.text, "OK")
            self.assertEqual(result.sequence, seq)

        result = parent.set_numerics("bogus")
        self.assertEqual(result.status, 1)
        self.assertEqual(result.text, "Invalid argument value for test")

    def test_C_basic_types_numeric_big(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        result = parent.set_numerics("big")
        self.assertEqual(result.status, 0)
        self.assertEqual(result.text, "OK")

        parent.update()

        self.assertEqual(parent.uint64val, 0x9494949449494949)
        self.assertEqual(parent.uint32val, 0xA5A55A5A)
        self.assertEqual(parent.uint16val, 0xB66B)
        self.assertEqual(parent.uint8val,  0xC7)

        self.assertEqual(parent.int64val, 1000000000000000000)
        self.assertEqual(parent.int32val, 1000000000)
        self.assertEqual(parent.int16val, 10000)
        self.assertEqual(parent.int8val,  100)

    def test_C_basic_types_numeric_small(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        result = parent.set_numerics("small")
        self.assertEqual(result.status, 0)
        self.assertEqual(result.text, "OK")

        parent.update()

        self.assertEqual(parent.uint64val, 4)
        self.assertEqual(parent.uint32val, 5)
        self.assertEqual(parent.uint16val, 6)
        self.assertEqual(parent.uint8val,  7)

        self.assertEqual(parent.int64val, 8)
        self.assertEqual(parent.int32val, 9)
        self.assertEqual(parent.int16val, 10)
        self.assertEqual(parent.int8val,  11)

    def test_C_basic_types_numeric_negative(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        result = parent.set_numerics("negative")
        self.assertEqual(result.status, 0)
        self.assertEqual(result.text, "OK")

        parent.update()

        self.assertEqual(parent.uint64val, 0)
        self.assertEqual(parent.uint32val, 0)
        self.assertEqual(parent.uint16val, 0)
        self.assertEqual(parent.uint8val,  0)

        self.assertEqual(parent.int64val, -10000000000)
        self.assertEqual(parent.int32val, -100000)
        self.assertEqual(parent.int16val, -1000)
        self.assertEqual(parent.int8val,  -100)

    def disabled_test_D_userid_for_method(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        result = parent.probe_userid()
        self.assertEqual(result.status, 0)
        self.assertEqual(result.userid, "guest")

    def test_D_get_by_object_id(self):
        self.startQmf()
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        newList = qmf.getObjects(_objectId=parent.getObjectId())
        self.assertEqual(len(newList), 1)

    def test_E_filter_by_object_id(self):
        self.startQmf()
        qmf = self.qmf

        list = qmf.getObjects(_class="exchange", name="qpid.management")
        self.assertEqual(len(list), 1, "No Management Exchange")
        mgmt_exchange = list[0]

        bindings = qmf.getObjects(_class="binding", exchangeRef=mgmt_exchange.getObjectId())
        if len(bindings) == 0:
            self.fail("No bindings found on management exchange")

        for binding in bindings:
            self.assertEqual(binding.exchangeRef, mgmt_exchange.getObjectId())

    def test_F_events(self):
        class Handler(qmf.console.Console):
            def __init__(self):
                self.queue = []

            def event(self, broker, event):
                if event.getClassKey().getClassName() == "test_event":
                    self.queue.append(event)

        handler = Handler()
        self.startQmf(handler)

        parents = self.qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        parent.set_numerics("big")
        parent.set_numerics("small")
        parent.set_numerics("negative")
        parent.set_short_string("TEST")
        parent.set_long_string("LONG_TEST")
        parent.probe_userid()

        queue = handler.queue
        self.assertEqual(len(queue), 5)
        self.assertEqual(queue[0].arguments["uint32val"], 0xA5A55A5A)
        self.assertEqual(queue[0].arguments["strval"], "Unused")

        # verify map and list event content.
        # see agent for structure of listval and mapval
        listval = queue[0].arguments["listval"]
        self.assertTrue(isinstance(listval, list))
        self.assertEqual(len(listval), 5)
        self.assertTrue(isinstance(listval[4], list))
        self.assertEqual(len(listval[4]), 4)
        self.assertTrue(isinstance(listval[4][3], dict))
        self.assertEqual(listval[4][3]["hi"], 10)
        self.assertEqual(listval[4][3]["lo"], 5)
        self.assertEqual(listval[4][3]["neg"], -3)

        mapval = queue[0].arguments["mapval"]
        self.assertTrue(isinstance(mapval, dict))
        self.assertEqual(len(mapval), 7)
        self.assertEqual(mapval['aLong'], 9999999999)
        self.assertEqual(mapval['aInt'], 54321)
        self.assertEqual(mapval['aSigned'], -666)
        self.assertEqual(mapval['aString'], "A String"),
        self.assertEqual(mapval['aFloat'], 3.1415),
        self.assertTrue(isinstance(mapval['aMap'], dict))
        self.assertEqual(len(mapval['aMap']), 2)
        self.assertEqual(mapval['aMap']['second'], 2)
        self.assertTrue(isinstance(mapval['aList'], list))
        self.assertEqual(len(mapval['aList']), 4)
        self.assertEqual(mapval['aList'][1], -1)

        self.assertEqual(queue[1].arguments["uint32val"], 5)
        self.assertEqual(queue[1].arguments["strval"], "Unused")
        self.assertEqual(queue[2].arguments["uint32val"], 0)
        self.assertEqual(queue[2].arguments["strval"], "Unused")
        self.assertEqual(queue[3].arguments["uint32val"], 0)
        self.assertEqual(queue[3].arguments["strval"], "TEST")
        self.assertEqual(queue[4].arguments["uint32val"], 0)
        self.assertEqual(queue[4].arguments["strval"], "LONG_TEST")



    def test_G_basic_map_list_data(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        # see agent for structure of listval

        self.assertTrue(isinstance(parent.listval, list))
        self.assertEqual(len(parent.listval), 5)
        self.assertTrue(isinstance(parent.listval[4], list))
        self.assertEqual(len(parent.listval[4]), 4)
        self.assertTrue(isinstance(parent.listval[4][3], dict))
        self.assertEqual(parent.listval[4][3]["hi"], 10)
        self.assertEqual(parent.listval[4][3]["lo"], 5)
        self.assertEqual(parent.listval[4][3]["neg"], -3)

        # see agent for structure of mapval

        self.assertTrue(isinstance(parent.mapval, dict))
        self.assertEqual(len(parent.mapval), 7)
        self.assertEqual(parent.mapval['aLong'], 9999999999)
        self.assertEqual(parent.mapval['aInt'], 54321)
        self.assertEqual(parent.mapval['aSigned'], -666)
        self.assertEqual(parent.mapval['aString'], "A String"),
        self.assertEqual(parent.mapval['aFloat'], 3.1415),
        self.assertTrue(isinstance(parent.mapval['aMap'], dict))
        self.assertEqual(len(parent.mapval['aMap']), 2)
        self.assertEqual(parent.mapval['aMap']['second'], 2)
        self.assertTrue(isinstance(parent.mapval['aList'], list))
        self.assertEqual(len(parent.mapval['aList']), 4)
        self.assertEqual(parent.mapval['aList'][1], -1)

    def test_H_map_list_method_call(self):
        self.startQmf();
        qmf = self.qmf

        parents = qmf.getObjects(_class="parent")
        self.assertEqual(len(parents), 1)
        parent = parents[0]

        inMap = {'aLong' : long(9999999999),
                 'aInt'  : int(54321),
                 'aSigned' : -666,
                 'aString' : "A String",
                 'aFloat' : 3.1415,
                 'aList' : ['x', -1, 'y', 2],
                 'abool' : False}
        inList = ['aString', long(1), -1, 2.7182, {'aMap': -8}, True]

        result = parent.test_map_list(inMap, inList)
        self.assertEqual(result.status, 0)
        self.assertEqual(result.text, "OK")

        # verify returned values
        self.assertEqual(len(inMap), len(result.outArgs['outMap']))
        for key,value in result.outArgs['outMap'].items():
            self.assertEqual(inMap[key], value)

        self.assertEqual(len(inList), len(result.outArgs['outList']))
        for idx in range(len(inList)):
            self.assertEqual(inList[idx], result.outArgs['outList'][idx])


    def getProperty(self, msg, name):
        for h in msg.headers:
            if hasattr(h, name): return getattr(h, name)
        return None            

    def getAppHeader(self, msg, name):
        headers = self.getProperty(msg, "application_headers")
        if headers:
            return headers[name]
        return None
