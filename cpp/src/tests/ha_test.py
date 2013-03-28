#!/usr/bin/env python

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

import os, signal, sys, time, imp, re, subprocess, glob, random, logging, shutil, math, unittest, random
import traceback
from qpid.messaging import Message, NotFound, ConnectionError, ReceiverError, Connection, Timeout, Disposition, REJECTED, Empty
from qpid.datatypes import uuid4, UUID
from brokertest import *
from threading import Thread, Lock, Condition
from logging import getLogger, WARN, ERROR, DEBUG, INFO
from qpidtoollibs import BrokerAgent

log = getLogger(__name__)

class LogLevel:
    """
    Temporarily change the log settings on the root logger.
    Used to suppress expected WARN messages from the python client.
    """
    def __init__(self, level):
        self.save_level = getLogger().getEffectiveLevel()
        getLogger().setLevel(level)

    def restore(self):
        getLogger().setLevel(self.save_level)

class QmfAgent(object):
    """Access to a QMF broker agent."""
    def __init__(self, address, **kwargs):
        self._connection = Connection.establish(
            address, client_properties={"qpid.ha-admin":1}, **kwargs)
        self._agent = BrokerAgent(self._connection)

    def __getattr__(self, name):
        a = getattr(self._agent, name)
        return a

class Credentials(object):
    """SASL credentials: username, password, and mechanism"""
    def __init__(self, username, password, mechanism):
        (self.username, self.password, self.mechanism) = (username, password, mechanism)

    def __str__(self): return "Credentials%s"%(self.tuple(),)

    def tuple(self): return (self.username, self.password, self.mechanism)

    def add_user(self, url): return "%s/%s@%s"%(self.username, self.password, url)

class HaBroker(Broker):
    """Start a broker with HA enabled
    @param client_cred: (user, password, mechanism) for admin clients started by the HaBroker.
    """
    def __init__(self, test, args=[], brokers_url=None, ha_cluster=True, ha_replicate="all",
                 client_credentials=None, **kwargs):
        assert BrokerTest.ha_lib, "Cannot locate HA plug-in"
        args = copy(args)
        args += ["--load-module", BrokerTest.ha_lib,
                 "--log-enable=debug+:ha::",
                 # FIXME aconway 2012-02-13: workaround slow link failover.
                 "--link-maintenance-interval=0.1",
                 "--ha-cluster=%s"%ha_cluster]
        if ha_replicate is not None:
            args += [ "--ha-replicate=%s"%ha_replicate ]
        if brokers_url: args += [ "--ha-brokers-url", brokers_url ]
        Broker.__init__(self, test, args, **kwargs)
        self.qpid_ha_path=os.path.join(os.getenv("PYTHON_COMMANDS"), "qpid-ha")
        assert os.path.exists(self.qpid_ha_path)
        self.qpid_config_path=os.path.join(os.getenv("PYTHON_COMMANDS"), "qpid-config")
        assert os.path.exists(self.qpid_config_path)
        self.qpid_ha_script=import_script(self.qpid_ha_path)
        self._agent = None
        self.client_credentials = client_credentials

    def __str__(self): return Broker.__str__(self)

    def qpid_ha(self, args):
        cred = self.client_credentials
        url = self.host_port()
        if cred:
            url =cred.add_user(url)
            args = args + ["--sasl-mechanism", cred.mechanism]
        self.qpid_ha_script.main_except(["", "-b", url]+args)

    def promote(self):
        self.ready(); self.qpid_ha(["promote"])
    def set_public_url(self, url): self.qpid_ha(["set", "--public-url", url])
    def set_brokers_url(self, url): self.qpid_ha(["set", "--brokers-url", url])
    def replicate(self, from_broker, queue): self.qpid_ha(["replicate", from_broker, queue])

    def agent(self):
        if not self._agent:
            cred = self.client_credentials
            if cred:
                self._agent = QmfAgent(cred.add_user(self.host_port()), sasl_mechanisms=cred.mechanism)
            else:
                self._agent = QmfAgent(self.host_port())
        return self._agent

    def qmf(self):
        hb = self.agent().getHaBroker()
        hb.update()
        return hb

    def ha_status(self): return self.qmf().status

    def wait_status(self, status):
        def try_get_status():
            self._status = "<unknown>"
            # Ignore ConnectionError, the broker may not be up yet.
            try:
                self._status = self.ha_status()
                return self._status == status;
            except ConnectionError: return False
        assert retry(try_get_status, timeout=20), "%s expected=%r, actual=%r"%(
            self, status, self._status)

    def wait_queue(self, queue, timeout=1):
        """ Wait for queue to be visible via QMF"""
        agent = self.agent()
        assert retry(lambda: agent.getQueue(queue) is not None, timeout=timeout)

    def wait_no_queue(self, queue, timeout=1):
        """ Wait for queue to be invisible via QMF"""
        agent = self.agent()
        assert retry(lambda: agent.getQueue(queue) is None, timeout=timeout)

    # FIXME aconway 2012-05-01: do direct python call to qpid-config code.
    def qpid_config(self, args):
        assert subprocess.call(
            [self.qpid_config_path, "--broker", self.host_port()]+args,
            stdout=1, stderr=subprocess.STDOUT
            ) == 0

    def config_replicate(self, from_broker, queue):
        self.qpid_config(["add", "queue", "--start-replica", from_broker, queue])

    def config_declare(self, queue, replication):
        self.qpid_config(["add", "queue", queue, "--replicate", replication])

    def connect_admin(self, **kwargs):
        cred = self.client_credentials
        if cred:
            return Broker.connect(
                self, client_properties={"qpid.ha-admin":1},
                username=cred.username, password=cred.password, sasl_mechanisms=cred.mechanism,
                **kwargs)
        else:
            return Broker.connect(self, client_properties={"qpid.ha-admin":1}, **kwargs)

    def wait_address(self, address):
        """Wait for address to become valid on the broker."""
        bs = self.connect_admin().session()
        try: wait_address(bs, address)
        finally: bs.connection.close()

    def wait_backup(self, address): self.wait_address(address)

    def assert_browse(self, queue, expected, **kwargs):
        """Verify queue contents by browsing."""
        bs = self.connect().session()
        try:
            wait_address(bs, queue)
            assert_browse_retry(bs, queue, expected, **kwargs)
        finally: bs.connection.close()

    def assert_browse_backup(self, queue, expected, **kwargs):
        """Combines wait_backup and assert_browse_retry."""
        bs = self.connect_admin().session()
        try:
            wait_address(bs, queue)
            assert_browse_retry(bs, queue, expected, **kwargs)
        finally: bs.connection.close()

    def assert_connect_fail(self):
        try:
            self.connect()
            self.test.fail("Expected ConnectionError")
        except ConnectionError: pass

    def try_connect(self):
        try: return self.connect()
        except ConnectionError: return None

    def ready(self):
        return Broker.ready(self, client_properties={"qpid.ha-admin":1})

    def kill(self):
        self._agent = None
        return Broker.kill(self)

class HaCluster(object):
    _cluster_count = 0

    def __init__(self, test, n, promote=True, wait=True, **kwargs):
        """Start a cluster of n brokers.

        @test: The test being run
        @n: start n brokers
        @promote: promote self[0] to primary
        @wait: wait for primary active and backups ready. Ignored if promote=False
        """
        self.test = test
        self.kwargs = kwargs
        self._brokers = []
        self.id = HaCluster._cluster_count
        self.broker_id = 0
        HaCluster._cluster_count += 1
        for i in xrange(n): self.start(False)
        self.update_urls()
        if promote:
            self[0].promote()
            if wait:
                self[0].wait_status("active")
                for b in self[1:]: b.wait_status("ready")


    def next_name(self):
        name="cluster%s-%s"%(self.id, self.broker_id)
        self.broker_id += 1
        return name

    def start(self, update_urls=True, args=[]):
        """Start a new broker in the cluster"""
        b = HaBroker(self.test, name=self.next_name(), **self.kwargs)
        b.ready()
        self._brokers.append(b)
        if update_urls: self.update_urls()
        return b

    def update_urls(self):
        self.url = ",".join([b.host_port() for b in self])
        if len(self) > 1:          # No failover addresses on a 1 cluster.
            for b in self:
                b.set_brokers_url(self.url)
                b.set_public_url(self.url)

    def connect(self, i):
        """Connect with reconnect_urls"""
        return self[i].connect(reconnect=True, reconnect_urls=self.url.split(","))

    def kill(self, i, promote_next=True):
        """Kill broker i, promote broker i+1"""
        self[i].kill()
        if promote_next: self[(i+1) % len(self)].promote()

    def restart(self, i):
        """Start a broker with the same port, name and data directory. It will get
        a separate log file: foo.n.log"""
        b = self._brokers[i]
        self._brokers[i] = HaBroker(
            self.test, name=b.name, port=b.port(), brokers_url=self.url,
            **self.kwargs)
        self._brokers[i].ready()

    def bounce(self, i, promote_next=True):
        """Stop and restart a broker in a cluster."""
        if (len(self) == 1):
            self.kill(i, promote_next=False)
            self.restart(i)
            self[i].ready()
            if promote_next: self[i].promote()
        else:
            self.kill(i, promote_next)
            self.restart(i)

    # Behave like a list of brokers.
    def __len__(self): return len(self._brokers)
    def __getitem__(self,index): return self._brokers[index]
    def __iter__(self): return self._brokers.__iter__()


def wait_address(session, address):
    """Wait for an address to become valid."""
    def check():
        try: session.sender(address); return True
        except NotFound: return False
    assert retry(check), "Timed out waiting for address %s"%(address)

def valid_address(session, address):
    """Test if an address is valid"""
    try:
        session.receiver(address)
        return True
    except NotFound: return False


