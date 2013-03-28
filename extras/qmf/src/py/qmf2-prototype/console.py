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
import os
import platform
import time
import datetime
import Queue
from logging import getLogger
from threading import Thread, Event
from threading import RLock
from threading import currentThread
from threading import Condition

from qpid.messaging import Connection, Message, Empty, SendError

from common import (QMF_APP_ID, OpCode, QmfQuery, Notifier, ContentType,
                    QmfData, QmfAddress, SchemaClass, SchemaClassId,
                    SchemaEventClass, SchemaObjectClass, WorkItem,
                    SchemaMethod, QmfEvent, timedelta_to_secs)


# global flag that indicates which thread (if any) is
# running the console notifier callback
_callback_thread=None


log = getLogger("qmf")
trace = getLogger("qmf.console")


##==============================================================================
## Console Transaction Management
##
## At any given time, a console application may have multiple outstanding
## message transactions with agents.  The following objects allow the console 
## to track these outstanding transactions.
##==============================================================================


class _Mailbox(object):
    """
    Virtual base class for all Mailbox-like objects.
    """
    def __init__(self, console):
        self.console = console
        self.cid = 0
        self.console._add_mailbox(self)

    def get_address(self):
        return self.cid

    def deliver(self, data):
        """
        Invoked by Console Management thread when a message arrives for
        this mailbox.
        """
        raise Exception("_Mailbox deliver() method must be provided")

    def destroy(self):
        """
        Release the mailbox.  Once called, the mailbox should no longer be
        referenced. 
        """
        self.console._remove_mailbox(self.cid)


class _SyncMailbox(_Mailbox):
    """
    A simple mailbox that allows a consumer to wait for delivery of data.
    """
    def __init__(self, console):
        """
        Invoked by application thread.
        """
        super(_SyncMailbox, self).__init__(console)
        self._cv = Condition()
        self._data = []
        self._waiting = False

    def deliver(self, data):
        """
        Drop data into the mailbox, waking any waiters if necessary.
        Invoked by Console Management thread only.
        """
        self._cv.acquire()
        try:
            self._data.append(data)
            # if was empty, notify waiters
            if len(self._data) == 1:
                self._cv.notify()
        finally:
            self._cv.release()

    def fetch(self, timeout=None):
        """
        Get one data item from a mailbox, with timeout.
        Invoked by application thread.
        """
        self._cv.acquire()
        try:
            if len(self._data) == 0:
                self._cv.wait(timeout)
            if len(self._data):
                return self._data.pop(0)
            return None
        finally:
            self._cv.release()


class _AsyncMailbox(_Mailbox):
    """
    A Mailbox for asynchronous delivery, with a timeout value.
    """
    def __init__(self, console, 
                 _timeout=None):
        """
        Invoked by application thread.
        """
        super(_AsyncMailbox, self).__init__(console)
        self.console = console

        if _timeout is None:
            _timeout = console._reply_timeout
        self.expiration_date = (datetime.datetime.utcnow() +
                                datetime.timedelta(seconds=_timeout))
        console._lock.acquire()
        try:
            console._async_mboxes[self.cid] = self
            console._next_mbox_expire = None
        finally:
            console._lock.release()

        # now that an async mbox has been created, wake the
        # console mgmt thread so it will know about the mbox expiration
        # date (and adjust its idle sleep period correctly)

        console._wake_thread()

    def reset_timeout(self, _timeout=None):
        """ Reset the expiration date for this mailbox.
        """
        if _timeout is None:
            _timeout = self.console._reply_timeout
        self.console._lock.acquire()
        try:
            self.expiration_date = (datetime.datetime.utcnow() +
                                    datetime.timedelta(seconds=_timeout))
            self.console._next_mbox_expire = None
        finally:
            self.console._lock.release()

        # wake the console mgmt thread so it will learn about the mbox
        # expiration date (and adjust its idle sleep period correctly)

        self.console._wake_thread()

    def deliver(self, msg):
        """
        """
        raise Exception("deliver() method must be provided")

    def expire(self):
        raise Exception("expire() method must be provided")


    def destroy(self):
        self.console._lock.acquire()
        try:
            if self.cid in self.console._async_mboxes:
                del self.console._async_mboxes[self.cid]
        finally:
            self.console._lock.release()
        super(_AsyncMailbox, self).destroy()



class _QueryMailbox(_AsyncMailbox):
    """
    A mailbox used for asynchronous query requests.
    """
    def __init__(self, console, 
                 agent_name,
                 context,
                 target,
                 _timeout=None):
        """
        Invoked by application thread.
        """
        super(_QueryMailbox, self).__init__(console,
                                            _timeout)
        self.agent_name = agent_name
        self.target = target
        self.context = context
        self.result = []

    def deliver(self, reply):
        """
        Process query response messages delivered to this mailbox.
        Invoked by Console Management thread only.
        """
        trace.debug("Delivering to query mailbox (agent=%s)." % self.agent_name)
        objects = reply.content
        if isinstance(objects, type([])):
            # convert from map to native types if needed
            if self.target == QmfQuery.TARGET_SCHEMA_ID:
                for sid_map in objects:
                    self.result.append(SchemaClassId.from_map(sid_map))

            elif self.target == QmfQuery.TARGET_SCHEMA:
                for schema_map in objects:
                    # extract schema id, convert based on schema type
                    sid_map = schema_map.get(SchemaClass.KEY_SCHEMA_ID)
                    if sid_map:
                        sid = SchemaClassId.from_map(sid_map)
                        if sid:
                            if sid.get_type() == SchemaClassId.TYPE_DATA:
                                schema = SchemaObjectClass.from_map(schema_map)
                            else:
                                schema = SchemaEventClass.from_map(schema_map)
                            self.console._add_schema(schema)  # add to schema cache
                            self.result.append(schema)

            elif self.target == QmfQuery.TARGET_OBJECT:
                for obj_map in objects:
                    # @todo: need the agent name - ideally from the
                    # reply message iself.
                    agent = self.console.get_agent(self.agent_name)
                    if agent:
                        obj = QmfConsoleData(map_=obj_map, agent=agent)
                        # start fetch of schema if not known
                        sid = obj.get_schema_class_id()
                        if sid:
                            self.console._prefetch_schema(sid, agent)
                        self.result.append(obj)


            else:
                # no conversion needed.
                self.result += objects

        if not "partial" in reply.properties:
            # log.error("QUERY COMPLETE for %s" % str(self.context))
            wi = WorkItem(WorkItem.QUERY_COMPLETE, self.context, self.result)
            self.console._work_q.put(wi)
            self.console._work_q_put = True

            self.destroy()


    def expire(self):
        trace.debug("Expiring query mailbox (agent=%s)." % self.agent_name)
        # send along whatever (possibly none) has been received so far
        wi = WorkItem(WorkItem.QUERY_COMPLETE, self.context, self.result)
        self.console._work_q.put(wi)
        self.console._work_q_put = True

        self.destroy()



class _SchemaPrefetchMailbox(_AsyncMailbox):
    """
    Handles responses to schema fetches made by the console.
    """
    def __init__(self, console,
                 schema_id,
                 _timeout=None):
        """
        Invoked by application thread.
        """
        super(_SchemaPrefetchMailbox, self).__init__(console,
                                                     _timeout)
        self.schema_id = schema_id

    def deliver(self, reply):
        """
        Process schema response messages.
        """
        trace.debug("Delivering schema mailbox (id=%s)." % self.schema_id)
        done = False
        schemas = reply.content
        if schemas and isinstance(schemas, type([])):
            for schema_map in schemas:
                # extract schema id, convert based on schema type
                sid_map = schema_map.get(SchemaClass.KEY_SCHEMA_ID)
                if sid_map:
                    sid = SchemaClassId.from_map(sid_map)
                    if sid:
                        if sid.get_type() == SchemaClassId.TYPE_DATA:
                            schema = SchemaObjectClass.from_map(schema_map)
                        else:
                            schema = SchemaEventClass.from_map(schema_map)
                        self.console._add_schema(schema)  # add to schema cache
        self.destroy()


    def expire(self):
        trace.debug("Expiring schema mailbox (id=%s)." % self.schema_id)
        self.destroy()



class _MethodMailbox(_AsyncMailbox):
    """
    A mailbox used for asynchronous method requests.
    """
    def __init__(self, console, 
                 context,
                 _timeout=None):
        """
        Invoked by application thread.
        """
        super(_MethodMailbox, self).__init__(console,
                                             _timeout)
        self.context = context

    def deliver(self, reply):
        """
        Process method response messages delivered to this mailbox.
        Invoked by Console Management thread only.
        """
        trace.debug("Delivering to method mailbox.")
        _map = reply.content
        if not _map or not isinstance(_map, type({})):
            log.error("Invalid method call reply message")
            result = None
        else:
            error=_map.get(SchemaMethod.KEY_ERROR)
            if error:
                error = QmfData.from_map(error)
                result = MethodResult(_error=error)
            else:
                result = MethodResult(_out_args=_map.get(SchemaMethod.KEY_ARGUMENTS))

        # create workitem
        wi = WorkItem(WorkItem.METHOD_RESPONSE, self.context, result)
        self.console._work_q.put(wi)
        self.console._work_q_put = True

        self.destroy()


    def expire(self):
        """
        The mailbox expired without receiving a reply.
        Invoked by the Console Management thread only.
        """
        trace.debug("Expiring method mailbox.")
        # send along an empty response
        wi = WorkItem(WorkItem.METHOD_RESPONSE, self.context, None)
        self.console._work_q.put(wi)
        self.console._work_q_put = True

        self.destroy()



class _SubscriptionMailbox(_AsyncMailbox):
    """
    A Mailbox for a single subscription.  Allows only sychronous "subscribe"
    and "refresh" requests.
    """
    def __init__(self, console, context, agent, duration, interval):
        """
        Invoked by application thread.
        """
        super(_SubscriptionMailbox, self).__init__(console, duration)
        self.cv = Condition()
        self.data = []
        self.result = []
        self.context = context
        self.duration = duration
        self.interval = interval
        self.agent_name = agent.get_name()
        self.agent_subscription_id = None          # from agent

    def subscribe(self, query):
        agent = self.console.get_agent(self.agent_name)
        if not agent:
            log.warning("subscribed failed - unknown agent '%s'" %
                            self.agent_name)
            return False
        try:
            trace.debug("Sending Subscribe to Agent (%s)" % self.agent_name)
            agent._send_subscribe_req(query, self.get_address(), self.interval,
                                      self.duration)
        except SendError, e:
            log.error(str(e))
            return False
        return True

    def resubscribe(self):
        agent = self.console.get_agent(self.agent_name)
        if not agent:
            log.warning("resubscribed failed - unknown agent '%s'",
                        self.agent_name)
            return False
        try:
            trace.debug("Sending resubscribe to Agent %s", self.agent_name)
            agent._send_resubscribe_req(self.get_address(),
                                        self.agent_subscription_id)
        except SendError, e:
            log.error(str(e))
            return False
        return True

    def deliver(self, msg):
        """
        """
        opcode = msg.properties.get("qmf.opcode")
        if (opcode == OpCode.subscribe_rsp):

            error = msg.content.get("_error")
            if error:
                try:
                    e_map = QmfData.from_map(error)
                except TypeError:
                    log.warning("Invalid QmfData map received: '%s'"
                                    % str(error))
                    e_map = QmfData.create({"error":"Unknown error"})
                sp = SubscribeParams(None, None, None, e_map)
            else:
                self.agent_subscription_id = msg.content.get("_subscription_id")
                self.duration = msg.content.get("_duration", self.duration)
                self.interval = msg.content.get("_interval", self.interval)
                self.reset_timeout(self.duration)
                sp = SubscribeParams(self.get_address(),
                                     self.interval,
                                     self.duration,
                                     None)
            self.cv.acquire()
            try:
                self.data.append(sp)
                # if was empty, notify waiters
                if len(self.data) == 1:
                    self.cv.notify()
            finally:
                self.cv.release()
            return

        # else: data indication
        agent_name = msg.properties.get("qmf.agent")
        if not agent_name:
            log.warning("Ignoring data_ind - no agent name given: %s" %
                            msg)
            return
        agent = self.console.get_agent(agent_name)
        if not agent:
            log.warning("Ignoring data_ind - unknown agent '%s'" %
                            agent_name)
            return

        objects = msg.content
        for obj_map in objects:
            obj = QmfConsoleData(map_=obj_map, agent=agent)
            # start fetch of schema if not known
            sid = obj.get_schema_class_id()
            if sid:
                self.console._prefetch_schema(sid, agent)
            self.result.append(obj)

        if not "partial" in msg.properties:
            wi = WorkItem(WorkItem.SUBSCRIBE_INDICATION, self.context, self.result)
            self.result = []
            self.console._work_q.put(wi)
            self.console._work_q_put = True

    def fetch(self, timeout=None):
        """
        Get one data item from a mailbox, with timeout.
        Invoked by application thread.
        """
        self.cv.acquire()
        try:
            if len(self.data) == 0:
                self.cv.wait(timeout)
            if len(self.data):
                return self.data.pop(0)
            return None
        finally:
            self.cv.release()

    def expire(self):
        """ The subscription expired.
        """
        self.destroy()




class _AsyncSubscriptionMailbox(_SubscriptionMailbox):
    """
    A Mailbox for a single subscription.  Allows only asychronous "subscribe"
    and "refresh" requests.
    """
    def __init__(self, console, context, agent, duration, interval):
        """
        Invoked by application thread.
        """
        super(_AsyncSubscriptionMailbox, self).__init__(console, context,
                                                        agent, duration,
                                                        interval)
        self.subscribe_pending = False

    def subscribe(self, query, reply_timeout):
        if super(_AsyncSubscriptionMailbox, self).subscribe(query):
            self.subscribe_pending = True
            self.reset_timeout(reply_timeout)
            return True
        return False

    def deliver(self, msg):
        """
        """
        super(_AsyncSubscriptionMailbox, self).deliver(msg)
        sp = self.fetch(0)
        if sp and self.subscribe_pending:
            wi = WorkItem(WorkItem.SUBSCRIBE_RESPONSE, self.context, sp)
            self.console._work_q.put(wi)
            self.console._work_q_put = True

            self.subscribe_pending = False

            if not sp.succeeded():
                self.destroy()


    def expire(self):
        """ Either the subscription expired, or a request timedout.
        """
        if self.subscribe_pending:
            wi = WorkItem(WorkItem.SUBSCRIBE_RESPONSE, self.context, None)
            self.console._work_q.put(wi)
            self.console._work_q_put = True
        self.destroy()


##==============================================================================
## DATA MODEL
##==============================================================================


class QmfConsoleData(QmfData):
    """
    Console's representation of an managed QmfData instance.  
    """
    def __init__(self, map_, agent):
        super(QmfConsoleData, self).__init__(_map=map_,
                                             _const=True) 
        self._agent = agent

    def get_timestamps(self): 
        """
        Returns a list of timestamps describing the lifecycle of
        the object.  All timestamps are represented by the AMQP
        timestamp type.  [0] = time of last update from Agent,
                         [1] = creation timestamp 
                         [2] = deletion timestamp, or zero if not
        deleted.
        """
        return [self._utime, self._ctime, self._dtime]

    def get_create_time(self): 
        """
        returns the creation timestamp
        """
        return self._ctime

    def get_update_time(self): 
        """
        returns the update timestamp
        """
        return self._utime

    def get_delete_time(self): 
        """
        returns the deletion timestamp, or zero if not yet deleted.
        """
        return self._dtime

    def is_deleted(self): 
        """
        True if deletion timestamp not zero.
        """
        return self._dtime != long(0)

    def refresh(self, _reply_handle=None, _timeout=None): 
        """
        request that the Agent update the value of this object's
        contents.
        """
        if _reply_handle is not None:
            log.error(" ASYNC REFRESH TBD!!!")
            return None

        assert self._agent
        assert self._agent._console

        if _timeout is None:
            _timeout = self._agent._console._reply_timeout

        # create query to agent using this objects ID
        query = QmfQuery.create_id_object(self.get_object_id(),
                                          self.get_schema_class_id())
        obj_list = self._agent._console.do_query(self._agent, query,
                                                _timeout=_timeout)
        if obj_list is None or len(obj_list) != 1:
            return None

        self._update(obj_list[0])
        return self


    def invoke_method(self, name, _in_args={}, _reply_handle=None,
                      _timeout=None):
        """
        Invoke the named method on this object.
        """
        assert self._agent
        assert self._agent._console

        oid = self.get_object_id()
        if oid is None:
            raise ValueError("Cannot invoke methods on unmanaged objects.")

        if _timeout is None:
            _timeout = self._agent._console._reply_timeout

        if _reply_handle is not None:
            mbox = _MethodMailbox(self._agent._console,
                                  _reply_handle)
        else:
            mbox = _SyncMailbox(self._agent._console)
        cid = mbox.get_address()

        _map = {self.KEY_OBJECT_ID:str(oid),
                SchemaMethod.KEY_NAME:name}

        sid = self.get_schema_class_id()
        if sid:
            _map[self.KEY_SCHEMA_ID] = sid.map_encode()
        if _in_args:
            _map[SchemaMethod.KEY_ARGUMENTS] = _in_args

        trace.debug("Sending method req to Agent (%s)" % time.time())
        try:
            self._agent._send_method_req(_map, cid)
        except SendError, e:
            log.error(str(e))
            mbox.destroy()
            return None

        if _reply_handle is not None:
            return True

        trace.debug("Waiting for response to method req (%s)" % _timeout)
        replyMsg = mbox.fetch(_timeout)
        mbox.destroy()

        if not replyMsg:
            trace.debug("Agent method req wait timed-out.")
            return None

        _map = replyMsg.content
        if not _map or not isinstance(_map, type({})):
            log.error("Invalid method call reply message")
            return None

        error=_map.get(SchemaMethod.KEY_ERROR)
        if error:
            return MethodResult(_error=QmfData.from_map(error))
        else:
            return MethodResult(_out_args=_map.get(SchemaMethod.KEY_ARGUMENTS))

    def _update(self, newer):
        super(QmfConsoleData,self).__init__(_values=newer._values, _subtypes=newer._subtypes,
                                           _tag=newer._tag, _object_id=newer._object_id,
                                           _ctime=newer._ctime, _utime=newer._utime, 
                                           _dtime=newer._dtime,
                                           _schema_id=newer._schema_id, _const=True)

class QmfLocalData(QmfData):
    """
    Console's representation of an unmanaged QmfData instance.  There
    is no remote agent associated with this instance. The Console has
    full control over this instance.
    """
    def __init__(self, values, _subtypes={}, _tag=None, _object_id=None,
                 _schema=None):
        # timestamp in millisec since epoch UTC
        ctime = long(time.time() * 1000)
        super(QmfLocalData, self).__init__(_values=values,
                                           _subtypes=_subtypes, _tag=_tag, 
                                           _object_id=_object_id,
                                           _schema=_schema, _ctime=ctime,
                                           _utime=ctime, _const=False)


class Agent(object):
    """
    A local representation of a remote agent managed by this console.
    """
    def __init__(self, name, console):
        """
        @type name: string
        @param name: uniquely identifies this agent in the AMQP domain.
        """

        if not isinstance(console, Console):
            raise TypeError("parameter must be an instance of class Console")

        self._name = name
        self._address = QmfAddress.direct(name, console._domain)
        self._console = console
        self._sender = None
        self._packages = {} # map of {package-name:[list of class-names], } for this agent
        self._subscriptions = [] # list of active standing subscriptions for this agent
        self._announce_timestamp = None # datetime when last announce received
        trace.debug( "Created Agent with address: [%s]" % self._address )


    def get_name(self):
        return self._name

    def is_active(self):
        return self._announce_timestamp != None

    def _send_msg(self, msg, correlation_id=None):
        """
        Low-level routine to asynchronously send a message to this agent.
        """
        msg.reply_to = str(self._console._address)
        if correlation_id:
            msg.correlation_id = str(correlation_id)
        # TRACE
        #log.error("!!! Console %s sending to agent %s (%s)" % 
        #              (self._console._name, self._name, str(msg)))
        self._sender.send(msg)
        # return handle

    def get_packages(self):
        """
        Return a list of the names of all packages known to this agent.
        """
        return self._packages.keys()

    def get_classes(self):
        """
        Return a dictionary [key:class] of classes known to this agent.
        """
        return self._packages.copy()

    def get_objects(self, query, kwargs={}):
        """
        Return a list of objects that satisfy the given query.

        @type query: dict, or common.Query
        @param query: filter for requested objects
        @type kwargs: dict
        @param kwargs: ??? used to build match selector and query ???
        @rtype: list
        @return: list of matching objects, or None.
        """
        pass

    def get_object(self, query, kwargs={}):
        """
        Get one object - query is expected to match only one object.
        ??? Recommended: explicit timeout param, default None ???

        @type query: dict, or common.Query
        @param query: filter for requested objects
        @type kwargs: dict
        @param kwargs: ??? used to build match selector and query ???
        @rtype: qmfConsole.ObjectProxy
        @return: one matching object, or none
        """
        pass


    def create_subscription(self, query):
        """
        Factory for creating standing subscriptions based on a given query.

        @type query: common.Query object
        @param query: determines the list of objects for which this subscription applies
        @rtype: qmfConsole.Subscription
        @returns: an object representing the standing subscription.
        """
        pass


    def invoke_method(self, name, _in_args={}, _reply_handle=None,
                      _timeout=None): 
        """
        Invoke the named method on this agent.
        """
        assert self._console

        if _timeout is None:
            _timeout = self._console._reply_timeout

        if _reply_handle is not None:
            mbox = _MethodMailbox(self._console,
                                  _reply_handle)
        else:
            mbox = _SyncMailbox(self._console)
        cid = mbox.get_address()

        _map = {SchemaMethod.KEY_NAME:name}
        if _in_args:
            _map[SchemaMethod.KEY_ARGUMENTS] = _in_args.copy()

        trace.debug("Sending method req to Agent (%s)" % time.time())
        try:
            self._send_method_req(_map, cid)
        except SendError, e:
            log.error(str(e))
            mbox.destroy()
            return None

        if _reply_handle is not None:
            return True

        trace.debug("Waiting for response to method req (%s)" % _timeout)
        replyMsg = mbox.fetch(_timeout)
        mbox.destroy()

        if not replyMsg:
            trace.debug("Agent method req wait timed-out.")
            return None

        _map = replyMsg.content
        if not _map or not isinstance(_map, type({})):
            log.error("Invalid method call reply message")
            return None

        return MethodResult(_out_args=_map.get(SchemaMethod.KEY_ARGUMENTS),
                            _error=_map.get(SchemaMethod.KEY_ERROR))

    def enable_events(self):
        raise Exception("enable_events tbd")

    def disable_events(self):
        raise Exception("disable_events tbd")

    def destroy(self):
        raise Exception("destroy tbd")

    def __repr__(self):
        return str(self._address)
    
    def __str__(self):
        return self.__repr__()

    def _send_query(self, query, correlation_id=None):
        """
        """
        msg = Message(id=QMF_APP_ID,
                      properties={"method":"request",
                                  "qmf.opcode":OpCode.query_req},
                      content=query.map_encode())
        self._send_msg( msg, correlation_id )


    def _send_method_req(self, mr_map, correlation_id=None):
        """
        """
        msg = Message(id=QMF_APP_ID,
                      properties={"method":"request",
                                  "qmf.opcode":OpCode.method_req},
                      content=mr_map)
        self._send_msg( msg, correlation_id )

    def _send_subscribe_req(self, query, correlation_id, _interval=None,
                            _lifetime=None):
        """
        """
        sr_map = {"_query":query.map_encode()}
        if _interval is not None:
            sr_map["_interval"] = _interval
        if _lifetime is not None:
            sr_map["_duration"] = _lifetime

        msg = Message(id=QMF_APP_ID,
                      properties={"method":"request",
                                  "qmf.opcode":OpCode.subscribe_req},
                      content=sr_map)
        self._send_msg(msg, correlation_id)


    def _send_resubscribe_req(self, correlation_id,
                              subscription_id):
        """
        """
        sr_map = {"_subscription_id":subscription_id}

        msg = Message(id=QMF_APP_ID,
                      properties={"method":"request",
                                  "qmf.opcode":OpCode.subscribe_refresh_ind},
                      content=sr_map)
        self._send_msg(msg, correlation_id)


    def _send_unsubscribe_ind(self, correlation_id, subscription_id):
        """
        """
        sr_map = {"_subscription_id":subscription_id}

        msg = Message(id=QMF_APP_ID,
                      properties={"method":"request",
                                  "qmf.opcode":OpCode.subscribe_cancel_ind},
                      content=sr_map)
        self._send_msg(msg, correlation_id)


  ##==============================================================================
  ## METHOD CALL
  ##==============================================================================

class MethodResult(object):
    def __init__(self, _out_args=None, _error=None):
        self._error = _error
        self._out_args = _out_args

    def succeeded(self): 
        return self._error is None

    def get_exception(self):
        return self._error

    def get_arguments(self): 
        return self._out_args

    def get_argument(self, name): 
        arg = None
        if self._out_args:
            arg = self._out_args.get(name)
        return arg



  ##==============================================================================
  ## SUBSCRIPTION
  ##==============================================================================

class SubscribeParams(object):
    """ Represents a standing subscription for this console.
    """
    def __init__(self, sid, interval, duration, _error=None):
        self._sid = sid
        self._interval = interval
        self._duration = duration
        self._error = _error

    def succeeded(self):
        return self._error is None

    def get_error(self):
        return self._error

    def get_subscription_id(self):
        return self._sid

    def get_publish_interval(self):
        return self._interval

    def get_duration(self):
        return self._duration


  ##==============================================================================
  ## CONSOLE
  ##==============================================================================






class Console(Thread):
    """
    A Console manages communications to a collection of agents on behalf of an application.
    """
    def __init__(self, name=None, _domain=None, notifier=None, 
                 reply_timeout = 60,
                 # agent_timeout = 120,
                 agent_timeout = 60,
                 kwargs={}):
        """
        @type name: str
        @param name: identifier for this console.  Must be unique.
        @type notifier: qmfConsole.Notifier
        @param notifier: invoked when events arrive for processing.
        @type kwargs: dict
        @param kwargs: ??? Unused
        """
        Thread.__init__(self)
        self._operational = False
        self._ready = Event()

        if not name:
            self._name = "qmfc-%s.%d" % (platform.node(), os.getpid())
        else:
            self._name = str(name)
        self._domain = _domain
        self._address = QmfAddress.direct(self._name, self._domain)
        self._notifier = notifier
        self._lock = RLock()
        self._conn = None
        self._session = None
        # dict of "agent-direct-address":class Agent entries
        self._agent_map = {}
        self._direct_recvr = None
        self._announce_recvr = None
        self._locate_sender = None
        self._schema_cache = {}
        self._pending_schema_req = []
        self._agent_discovery_filter = None
        self._reply_timeout = reply_timeout
        self._agent_timeout = agent_timeout
        self._subscribe_timeout = 300  # @todo: parameterize
        self._next_agent_expire = None
        self._next_mbox_expire = None
        # for passing WorkItems to the application
        self._work_q = Queue.Queue()
        self._work_q_put = False
        # Correlation ID and mailbox storage
        self._correlation_id = long(time.time())  # pseudo-randomize
        self._post_office = {} # indexed by cid
        self._async_mboxes = {} # indexed by cid, used to expire them

    def destroy(self, timeout=None):
        """
        Must be called before the Console is deleted.  
        Frees up all resources and shuts down all background threads.

        @type timeout: float
        @param timeout: maximum time in seconds to wait for all background threads to terminate.  Default: forever.
        """
        trace.debug("Destroying Console...")
        if self._conn:
            self.remove_connection(self._conn, timeout)
        trace.debug("Console Destroyed")

    def add_connection(self, conn):
        """
        Add a AMQP connection to the console.  The console will setup a session over the
        connection.  The console will then broadcast an Agent Locate Indication over
        the session in order to discover present agents.

        @type conn: qpid.messaging.Connection
        @param conn: the connection to the AMQP messaging infrastructure.
        """
        if self._conn:
            raise Exception( "Multiple connections per Console not supported." );
        self._conn = conn
        self._session = conn.session(name=self._name)

        # for messages directly addressed to me
        self._direct_recvr = self._session.receiver(str(self._address) +
                                                    ";{create:always,"
                                                    " node:"
                                                    " {type:topic,"
                                                    " x-declare:"
                                                    " {type:direct}}}", 
                                                    capacity=1)
        trace.debug("my direct addr=%s" % self._direct_recvr.source)

        self._direct_sender = self._session.sender(str(self._address.get_node()) +
                                                   ";{create:always,"
                                                   " node:"
                                                   " {type:topic,"
                                                   " x-declare:"
                                                   " {type:direct}}}")
        trace.debug("my direct sender=%s" % self._direct_sender.target)

        # for receiving "broadcast" messages from agents
        default_addr = QmfAddress.topic(QmfAddress.SUBJECT_AGENT_IND + ".#", 
                                        self._domain)
        self._topic_recvr = self._session.receiver(str(default_addr) +
                                                   ";{create:always,"
                                                   " node:{type:topic}}",
                                                   capacity=1)
        trace.debug("default topic recv addr=%s" % self._topic_recvr.source)


        # for sending to topic subscribers
        topic_addr = QmfAddress.topic(QmfAddress.SUBJECT_CONSOLE_IND, self._domain)
        self._topic_sender = self._session.sender(str(topic_addr) +
                                                  ";{create:always,"
                                                  " node:{type:topic}}")
        trace.debug("default topic send addr=%s" % self._topic_sender.target)

        #
        # Now that receivers are created, fire off the receive thread...
        #
        self._operational = True
        self.start()
        self._ready.wait(10)
        if not self._ready.isSet():
            raise Exception("Console managment thread failed to start.")



    def remove_connection(self, conn, timeout=None):
        """
        Remove an AMQP connection from the console.  Un-does the add_connection() operation,
        and releases any agents and sessions associated with the connection.

        @type conn: qpid.messaging.Connection
        @param conn: connection previously added by add_connection()
        """
        if self._conn and conn and conn != self._conn:
            log.error( "Attempt to delete unknown connection: %s" % str(conn))

        # tell connection thread to shutdown
        self._operational = False
        if self.isAlive():
            # kick my thread to wake it up
            self._wake_thread()
            trace.debug("waiting for console receiver thread to exit")
            self.join(timeout)
            if self.isAlive():
                log.error( "Console thread '%s' is hung..." % self.getName() )
        self._direct_recvr.close()
        self._direct_sender.close()
        self._topic_recvr.close()
        self._topic_sender.close()
        self._session.close()
        self._session = None
        self._conn = None
        trace.debug("console connection removal complete")


    def get_address(self):
        """
        The AMQP address this Console is listening to.
        """
        return self._address


    def destroy_agent( self, agent ):
        """
        Undoes create.
        """
        if not isinstance(agent, Agent):
            raise TypeError("agent must be an instance of class Agent")

        self._lock.acquire()
        try:
            if agent._name in self._agent_map:
                del self._agent_map[agent._name]
        finally:
            self._lock.release()

    def find_agent(self, name, timeout=None ):
        """
        Given the name of a particular agent, return an instance of class Agent
        representing that agent.  Return None if the agent does not exist.
        """

        self._lock.acquire()
        try:
            agent = self._agent_map.get(name)
            if agent:
                return agent
        finally:
            self._lock.release()

        # agent not present yet - ping it with an agent_locate

        mbox = _SyncMailbox(self)
        cid = mbox.get_address()

        query = QmfQuery.create_id(QmfQuery.TARGET_AGENT, name)
        msg = Message(id=QMF_APP_ID,
                      subject="console.ind.locate." + name,
                      properties={"method":"request",
                                  "qmf.opcode":OpCode.agent_locate_req},
                      content=query._predicate)
        msg.content_type="amqp/list"
        msg.reply_to = str(self._address)
        msg.correlation_id = str(cid)
        trace.debug("%s Sending Agent Locate (%s)", self._name, str(msg))
        try:
            self._topic_sender.send(msg)
        except SendError, e:
            log.error(str(e))
            mbox.destroy()
            return None

        if timeout is None:
            timeout = self._reply_timeout

        new_agent = None
        trace.debug("Waiting for response to Agent Locate (%s)" % timeout)
        mbox.fetch(timeout)
        mbox.destroy()
        trace.debug("Agent Locate wait ended (%s)" % time.time())
        self._lock.acquire()
        try:
            new_agent = self._agent_map.get(name)
        finally:
            self._lock.release()

        return new_agent


    def get_agents(self):
        """
        Return the list of known agents.
        """
        self._lock.acquire()
        try:
            agents = self._agent_map.values()
        finally:
            self._lock.release()
        return agents


    def get_agent(self, name):
        """
        Return the named agent, else None if not currently available.
        """
        self._lock.acquire()
        try:
            agent = self._agent_map.get(name)
        finally:
            self._lock.release()
        return agent


    def do_query(self, agent, query, _reply_handle=None, _timeout=None ):
        """
        """
        target = query.get_target()

        if _reply_handle is not None:
            mbox = _QueryMailbox(self,
                                 agent.get_name(),
                                 _reply_handle,
                                 target,
                                 _timeout)
        else:
            mbox = _SyncMailbox(self)

        cid = mbox.get_address()

        try:
            trace.debug("Sending Query to Agent (%s)" % time.time())
            agent._send_query(query, cid)
        except SendError, e:
            log.error(str(e))
            mbox.destroy()
            return None

        # return now if async reply expected
        if _reply_handle is not None:
            return True

        if not _timeout:
            _timeout = self._reply_timeout

        trace.debug("Waiting for response to Query (%s)" % _timeout)
        now = datetime.datetime.utcnow()
        expire =  now + datetime.timedelta(seconds=_timeout)

        response = []
        while (expire > now):
            _timeout = timedelta_to_secs(expire - now)
            reply = mbox.fetch(_timeout)
            if not reply:
                trace.debug("Query wait timed-out.")
                break

            objects = reply.content
            if not objects or not isinstance(objects, type([])):
                break

            # convert from map to native types if needed
            if target == QmfQuery.TARGET_SCHEMA_ID:
                for sid_map in objects:
                    response.append(SchemaClassId.from_map(sid_map))

            elif target == QmfQuery.TARGET_SCHEMA:
                for schema_map in objects:
                    # extract schema id, convert based on schema type
                    sid_map = schema_map.get(SchemaClass.KEY_SCHEMA_ID)
                    if sid_map:
                        sid = SchemaClassId.from_map(sid_map)
                        if sid:
                            if sid.get_type() == SchemaClassId.TYPE_DATA:
                                schema = SchemaObjectClass.from_map(schema_map)
                            else:
                                schema = SchemaEventClass.from_map(schema_map)
                            self._add_schema(schema)  # add to schema cache
                            response.append(schema)

            elif target == QmfQuery.TARGET_OBJECT:
                for obj_map in objects:
                    obj = QmfConsoleData(map_=obj_map, agent=agent)
                    # start fetch of schema if not known
                    sid = obj.get_schema_class_id()
                    if sid:
                        self._prefetch_schema(sid, agent)
                    response.append(obj)
            else:
                # no conversion needed.
                response += objects

            if not "partial" in reply.properties:
                # reply not broken up over multiple msgs
                break

            now = datetime.datetime.utcnow()

        mbox.destroy()
        return response


    def create_subscription(self, agent, query, console_handle,
                            _interval=None, _duration=None,
                            _blocking=True, _timeout=None):
        if not _duration:
            _duration = self._subscribe_timeout

        if _timeout is None:
            _timeout = self._reply_timeout

        if not _blocking:
            mbox = _AsyncSubscriptionMailbox(self, console_handle, agent,
                                             _duration, _interval)
            if not mbox.subscribe(query, _timeout):
                mbox.destroy()
                return False
            return True
        else:
            mbox = _SubscriptionMailbox(self, console_handle, agent, _duration,
                                        _interval)

            if not mbox.subscribe(query):
                mbox.destroy()
                return None

            trace.debug("Waiting for response to subscription (%s)" % _timeout)
            # @todo: what if mbox expires here?
            sp = mbox.fetch(_timeout)

            if not sp:
                trace.debug("Subscription request wait timed-out.")
                mbox.destroy()
                return None

            if not sp.succeeded():
                mbox.destroy()

            return sp

    def refresh_subscription(self, subscription_id,
                             _duration=None,
                             _timeout=None):
        if _timeout is None:
            _timeout = self._reply_timeout

        mbox = self._get_mailbox(subscription_id)
        if not mbox:
            log.warning("Subscription %s not found." % subscription_id)
            return None

        if isinstance(mbox, _AsyncSubscriptionMailbox):
            return mbox.resubscribe()
        else:
            # synchronous - wait for reply
            if not mbox.resubscribe():
                # @todo ???? mbox.destroy()
                return None

            # wait for reply

            trace.debug("Waiting for response to subscription (%s)" % _timeout)
            sp = mbox.fetch(_timeout)

            if not sp:
                trace.debug("re-subscribe request wait timed-out.")
                # @todo???? mbox.destroy()
                return None

            return sp


    def cancel_subscription(self, subscription_id):
        """
        """
        mbox = self._get_mailbox(subscription_id)
        if not mbox:
            return

        agent = self.get_agent(mbox.agent_name)
        if agent:
            try:
                trace.debug("Sending UnSubscribe to Agent (%s)" % time.time())
                agent._send_unsubscribe_ind(subscription_id,
                                            mbox.agent_subscription_id)
            except SendError, e:
                log.error(str(e))

        mbox.destroy()


    def _wake_thread(self):
        """
        Make the console management thread loop wakeup from its next_receiver
        sleep.
        """
        trace.debug("Sending noop to wake up [%s]" % self._address)
        msg = Message(id=QMF_APP_ID,
                      subject=self._name,
                      properties={"method":"indication",
                                  "qmf.opcode":OpCode.noop},
                      content={})
        try:
            self._direct_sender.send( msg, sync=True )
        except SendError, e:
            log.error(str(e))


    def run(self):
        """
        Console Management Thread main loop.
        Handles inbound messages, agent discovery, async mailbox timeouts.
        """
        global _callback_thread

        self._ready.set()

        while self._operational:

            # qLen = self._work_q.qsize()

            while True:
                try:
                    msg = self._topic_recvr.fetch(timeout=0)
                except Empty:
                    break
                # TRACE:
                # log.error("!!! Console %s: msg on %s [%s]" %
                # (self._name, self._topic_recvr.source, msg))
                self._dispatch(msg, _direct=False)

            while True:
                try:
                    msg = self._direct_recvr.fetch(timeout = 0)
                except Empty:
                    break
                # TRACE
                #log.error("!!! Console %s: msg on %s [%s]" %
                # (self._name, self._direct_recvr.source, msg))
                self._dispatch(msg, _direct=True)

            self._expire_agents()   # check for expired agents
            self._expire_mboxes()   # check for expired async mailbox requests

            #if qLen == 0 and self._work_q.qsize() and self._notifier:
            if self._work_q_put and self._notifier:
                # new stuff on work queue, kick the the application...
                self._work_q_put = False
                _callback_thread = currentThread()
                trace.debug("Calling console notifier.indication")
                self._notifier.indication()
                _callback_thread = None


            # wait for a message to arrive, or an agent
            # to expire, or a mailbox requrest to time out
            now = datetime.datetime.utcnow()
            next_expire = self._next_agent_expire

            self._lock.acquire()
            try:
            # the mailbox expire flag may be cleared by the
            # app thread(s) to force an immedate mailbox scan
                if self._next_mbox_expire is None:
                    next_expire = now
                elif self._next_mbox_expire < next_expire:
                    next_expire = self._next_mbox_expire
            finally:
                self._lock.release()

            timeout = timedelta_to_secs(next_expire - now)

            if self._operational and timeout > 0.0:
                try:
                    trace.debug("waiting for next rcvr (timeout=%s)..." % timeout)
                    self._session.next_receiver(timeout = timeout)
                except Empty:
                    pass

        trace.debug("Shutting down Console thread")

    def get_objects(self,
                    _object_id=None,
                    _schema_id=None,
                    _pname=None, _cname=None,
                    _agents=None,
                    _timeout=None):
        """
        Retrieve objects by id or schema.

        By object_id: must specify schema_id or pname & cname if object defined
        by a schema.  Undescribed objects: only object_id needed.
        
        By schema: must specify schema_id or pname & cname - all instances of
        objects defined by that schema are returned.
        """
        if _agents is None:
            # use copy of current agent list
            self._lock.acquire()
            try:
                agent_list = self._agent_map.values()
            finally:
                self._lock.release()
        elif isinstance(_agents, Agent):
            agent_list = [_agents]
        else:
            agent_list = _agents
            # @todo validate this list!

        if _timeout is None:
            _timeout = self._reply_timeout

        # @todo: fix when async do_query done - query all agents at once, then
        # wait for replies, instead of per-agent querying....

        obj_list = []
        expired = datetime.datetime.utcnow() + datetime.timedelta(seconds=_timeout)
        for agent in agent_list:
            if not agent.is_active():
                continue
            now = datetime.datetime.utcnow()
            if now >= expired:
                break

            if _pname is None:
                if _object_id:
                    query = QmfQuery.create_id_object(_object_id,
                                                      _schema_id)
                else:
                    if _schema_id is not None:
                        t_params = {QmfData.KEY_SCHEMA_ID: _schema_id}
                    else:
                        t_params = None
                    query = QmfQuery.create_wildcard(QmfQuery.TARGET_OBJECT,
                                                     t_params)
                timeout = timedelta_to_secs(expired - now)
                reply = self.do_query(agent, query, _timeout=timeout)
                if reply:
                    obj_list = obj_list + reply
            else:
                # looking up by package name (and maybe class name), need to 
                # find all schema_ids in that package, then lookup object by
                # schema_id
                if _cname is not None:
                    pred = [QmfQuery.AND,
                            [QmfQuery.EQ,
                             SchemaClassId.KEY_PACKAGE,
                             [QmfQuery.QUOTE, _pname]],
                            [QmfQuery.EQ, SchemaClassId.KEY_CLASS,
                             [QmfQuery.QUOTE, _cname]]]
                else:
                    pred = [QmfQuery.EQ,
                            SchemaClassId.KEY_PACKAGE,
                            [QmfQuery.QUOTE, _pname]]
                query = QmfQuery.create_predicate(QmfQuery.TARGET_SCHEMA_ID, pred)
                timeout = timedelta_to_secs(expired - now)
                sid_list = self.do_query(agent, query, _timeout=timeout)
                if sid_list:
                    for sid in sid_list:
                        now = datetime.datetime.utcnow()
                        if now >= expired:
                            break
                        if _object_id is not None:
                            query = QmfQuery.create_id_object(_object_id, sid)
                        else:
                            t_params = {QmfData.KEY_SCHEMA_ID: sid}
                            query = QmfQuery.create_wildcard(QmfQuery.TARGET_OBJECT, t_params)
                        timeout = timedelta_to_secs(expired - now)
                        reply = self.do_query(agent, query, _timeout=timeout)
                        if reply:
                            obj_list = obj_list + reply
        if obj_list:
            return obj_list
        return None



    # called by run() thread ONLY
    #
    def _dispatch(self, msg, _direct=True):
        """
        PRIVATE: Process a message received from an Agent
        """
        trace.debug( "Message received from Agent! [%s]", msg )

        opcode = msg.properties.get("qmf.opcode")
        if not opcode:
            log.error("Ignoring unrecognized message '%s'", msg)
            return
        version = 2 # @todo: fix me

        cmap = {}; props = {}
        if msg.content_type == "amqp/map":
            cmap = msg.content
        if msg.properties:
            props = msg.properties

        if opcode == OpCode.agent_heartbeat_ind:
            self._handle_agent_ind_msg( msg, cmap, version, _direct )
        elif opcode == OpCode.agent_locate_rsp:
            self._handle_agent_ind_msg( msg, cmap, version, _direct )
        elif msg.correlation_id:
            self._handle_response_msg(msg, cmap, version, _direct)
        elif opcode == OpCode.data_ind:
            self._handle_indication_msg(msg, cmap, version, _direct)
        elif opcode == OpCode.noop:
             trace.debug("No-op msg received.")
        else:
            log.warning("Ignoring message with unrecognized 'opcode' value: '%s'", opcode)


    def _handle_agent_ind_msg(self, msg, cmap, version, direct):
        """
        Process a received agent-ind message.  This message may be a response to a
        agent-locate, or it can be an unsolicited agent announce.
        """

        trace.debug("%s _handle_agent_ind_msg '%s'", self._name, str(msg))

        try:
            tmp = QmfData.from_map(msg.content)
        except:
            log.warning("%s invalid Agent Indication msg format '%s'",
                        self._name, str(msg))
            return

        try:
            name = tmp.get_value("_name")
        except:
            log.warning("Bad Agent ind msg received: %s", str(msg))
            return

        correlated = False
        if msg.correlation_id:
            mbox = self._get_mailbox(msg.correlation_id)
            correlated = mbox is not None

        agent = None
        self._lock.acquire()
        try:
            agent = self._agent_map.get(name)
            if agent:
                # agent already known, just update timestamp
                agent._announce_timestamp = datetime.datetime.utcnow()
        finally:
            self._lock.release()

        if not agent:
            # need to create and add a new agent?
            matched = False
            if self._agent_discovery_filter:
                matched = self._agent_discovery_filter.evaluate(tmp)

            if (correlated or matched):
                agent = self._create_agent(name)
                if not agent:
                    return   # failed to add agent
                agent._announce_timestamp = datetime.datetime.utcnow()

                if matched:
                    # unsolicited, but newly discovered
                    trace.debug("AGENT_ADDED for %s (%s)" % (agent, time.time()))
                    wi = WorkItem(WorkItem.AGENT_ADDED, None, {"agent": agent})
                    self._work_q.put(wi)
                    self._work_q_put = True

        if correlated:
            # wake up all waiters
            trace.debug("waking waiters for correlation id %s" % msg.correlation_id)
            mbox.deliver(msg)

    def _handle_response_msg(self, msg, cmap, version, direct):
        """
        Process a received data-ind message.
        """
        trace.debug("%s _handle_response_msg '%s'", self._name, str(msg))

        mbox = self._get_mailbox(msg.correlation_id)
        if not mbox:
            log.warning("%s Response msg received with unknown correlation_id"
                            " msg='%s'", self._name, str(msg))
            return

        # wake up all waiters
        trace.debug("waking waiters for correlation id %s" % msg.correlation_id)
        mbox.deliver(msg)

    def _handle_indication_msg(self, msg, cmap, version, _direct):

        aname = msg.properties.get("qmf.agent")
        if not aname:
            trace.debug("No agent name field in indication message.")
            return

        content_type = msg.properties.get("qmf.content")
        if (content_type != ContentType.event or
            not isinstance(msg.content, type([]))):
            log.warning("Bad event indication message received: '%s'", msg)
            return

        emap = msg.content[0]
        if not isinstance(emap, type({})):
            trace.debug("Invalid event body in indication message: '%s'", msg)
            return

        agent = None
        self._lock.acquire()
        try:
            agent = self._agent_map.get(aname)
        finally:
            self._lock.release()
        if not agent:
            trace.debug("Agent '%s' not known." % aname)
            return
        try:
            # @todo: schema???
            event = QmfEvent.from_map(emap)
        except TypeError:
            trace.debug("Invalid QmfEvent map received: %s" % str(emap))
            return

        # @todo: schema?  Need to fetch it, but not from this thread!
        # This thread can not pend on a request.
        trace.debug("Publishing event received from agent %s" % aname)
        wi = WorkItem(WorkItem.EVENT_RECEIVED, None,
                      {"agent":agent,
                       "event":event})
        self._work_q.put(wi)
        self._work_q_put = True


    def _expire_mboxes(self):
        """
        Check all async mailboxes for outstanding requests that have expired.
        """
        self._lock.acquire()
        try:
            now = datetime.datetime.utcnow()
            if self._next_mbox_expire and now < self._next_mbox_expire:
                return
            expired_mboxes = []
            self._next_mbox_expire = None
            for mbox in self._async_mboxes.itervalues():
                if now >= mbox.expiration_date:
                    expired_mboxes.append(mbox)
                else:
                    if (self._next_mbox_expire is None or
                        mbox.expiration_date < self._next_mbox_expire):
                        self._next_mbox_expire = mbox.expiration_date

            for mbox in expired_mboxes:
                del self._async_mboxes[mbox.cid]
        finally:
            self._lock.release()

        for mbox in expired_mboxes:
            # note: expire() may deallocate the mbox, so don't touch
            # it further.
            mbox.expire()


    def _expire_agents(self):
        """
        Check for expired agents and issue notifications when they expire.
        """
        now = datetime.datetime.utcnow()
        if self._next_agent_expire and now < self._next_agent_expire:
            return
        lifetime_delta = datetime.timedelta(seconds = self._agent_timeout)
        next_expire_delta = lifetime_delta
        self._lock.acquire()
        try:
            trace.debug("!!! expiring agents '%s'" % now)
            for agent in self._agent_map.itervalues():
                if agent._announce_timestamp:
                    agent_deathtime = agent._announce_timestamp + lifetime_delta
                    if agent_deathtime <= now:
                        trace.debug("AGENT_DELETED for %s" % agent)
                        agent._announce_timestamp = None
                        wi = WorkItem(WorkItem.AGENT_DELETED, None,
                                      {"agent":agent})
                        # @todo: remove agent from self._agent_map
                        self._work_q.put(wi)
                        self._work_q_put = True
                    else:
                        if (agent_deathtime - now) < next_expire_delta:
                            next_expire_delta = agent_deathtime - now

            self._next_agent_expire = now + next_expire_delta
            trace.debug("!!! next expire cycle = '%s'" % self._next_agent_expire)
        finally:
            self._lock.release()



    def _create_agent( self, name ):
        """
        Factory to create/retrieve an agent for this console
        """
        trace.debug("creating agent %s" % name)
        self._lock.acquire()
        try:
            agent = self._agent_map.get(name)
            if agent:
                return agent

            agent = Agent(name, self)
            try:
                agent._sender = self._session.sender(str(agent._address) + 
                                                     ";{create:always,"
                                                     " node:"
                                                     " {type:topic,"
                                                     " x-declare:"
                                                     " {type:direct}}}") 
            except:
                log.warning("Unable to create sender for %s" % name)
                return None
            trace.debug("created agent sender %s" % agent._sender.target)

            self._agent_map[name] = agent
        finally:
            self._lock.release()

        # new agent - query for its schema database for
        # seeding the schema cache (@todo)
        # query = QmfQuery({QmfQuery.TARGET_SCHEMA_ID:None})
        # agent._sendQuery( query )

        return agent



    def enable_agent_discovery(self, _query=None):
        """
        Called to enable the asynchronous Agent Discovery process.
        Once enabled, AGENT_ADD work items can arrive on the WorkQueue.
        """
        # @todo: fix - take predicate only, not entire query!
        if _query is not None:
            if (not isinstance(_query, QmfQuery) or
                _query.get_target() != QmfQuery.TARGET_AGENT):
                raise TypeError("Type QmfQuery with target == TARGET_AGENT expected")
            self._agent_discovery_filter = _query
        else:
            # create a match-all agent query (no predicate)
            self._agent_discovery_filter = QmfQuery.create_wildcard(QmfQuery.TARGET_AGENT) 

    def disable_agent_discovery(self):
        """
        Called to disable the async Agent Discovery process enabled by
        calling enableAgentDiscovery()
        """
        self._agent_discovery_filter = None



    def get_workitem_count(self):
        """
        Returns the count of pending WorkItems that can be retrieved.
        """
        return self._work_q.qsize()



    def get_next_workitem(self, timeout=None):
        """
        Returns the next pending work item, or None if none available.
        @todo: subclass and return an Empty event instead.
        """
        try:
            wi = self._work_q.get(True, timeout)
        except Queue.Empty:
            return None
        return wi


    def release_workitem(self, wi):
        """
        Return a WorkItem to the Console when it is no longer needed.
        @todo: call Queue.task_done() - only 2.5+

        @type wi: class qmfConsole.WorkItem
        @param wi: work item object to return.
        """
        pass

    def _add_schema(self, schema):
        """
        @todo
        """
        if not isinstance(schema, SchemaClass):
            raise TypeError("SchemaClass type expected")

        self._lock.acquire()
        try:
            sid = schema.get_class_id()
            if not self._schema_cache.has_key(sid):
                self._schema_cache[sid] = schema
                if sid in self._pending_schema_req:
                    self._pending_schema_req.remove(sid)
        finally:
            self._lock.release()

    def _prefetch_schema(self, schema_id, agent):
        """
        Send an async request for the schema identified by schema_id if the
        schema is not available in the cache.
        """
        need_fetch = False
        self._lock.acquire()
        try:
            if ((not self._schema_cache.has_key(schema_id)) and
                schema_id not in self._pending_schema_req):
                self._pending_schema_req.append(schema_id)
                need_fetch = True
        finally:
            self._lock.release()

        if need_fetch:
            mbox = _SchemaPrefetchMailbox(self, schema_id)
            query = QmfQuery.create_id(QmfQuery.TARGET_SCHEMA, schema_id)
            trace.debug("Sending Schema Query to Agent (%s)" % time.time())
            try:
                agent._send_query(query, mbox.get_address())
            except SendError, e:
                log.error(str(e))
                mbox.destroy()
                self._lock.acquire()
                try:
                    self._pending_schema_req.remove(schema_id)
                finally:
                    self._lock.release()


    def _fetch_schema(self, schema_id, _agent=None, _timeout=None):
        """
        Find the schema identified by schema_id.  If not in the cache, ask the
        agent for it.
        """
        if not isinstance(schema_id, SchemaClassId):
            raise TypeError("SchemaClassId type expected")

        self._lock.acquire()
        try:
            schema = self._schema_cache.get(schema_id)
            if schema:
                return schema
        finally:
            self._lock.release()

        if _agent is None:
            return None

        # note: do_query will add the new schema to the cache automatically.
        slist = self.do_query(_agent,
                              QmfQuery.create_id(QmfQuery.TARGET_SCHEMA, schema_id),
                              _timeout=_timeout)
        if slist:
            return slist[0]
        else:
            return None

    def _add_mailbox(self, mbox):
        """ 
        Add a mailbox to the post office, and assign it a unique address.
        """
        self._lock.acquire()
        try:
            mbox.cid = self._correlation_id
            self._correlation_id += 1
            self._post_office[mbox.cid] = mbox
        finally:
            self._lock.release()

    def _get_mailbox(self, mid):
        try:
            mid = long(mid)
        except TypeError:
            log.error("Invalid mailbox id: %s" % str(mid))
            return None

        self._lock.acquire()
        try:
            return self._post_office.get(mid)
        finally:
            self._lock.release()


    def _remove_mailbox(self, mid):
        """ Remove a mailbox and its address from the post office """
        try:
            mid = long(mid)
        except TypeError:
            log.error("Invalid mailbox id: %s" % str(mid))
            return None

        self._lock.acquire()
        try:
            if mid in self._post_office:
                del self._post_office[mid]
        finally:
            self._lock.release()

    def __repr__(self):
        return str(self._address)

    # def get_packages(self):
    #     plist = []
    #     for i in range(self.impl.packageCount()):
    #         plist.append(self.impl.getPackageName(i))
    #     return plist
    
    
    # def get_classes(self, package, kind=CLASS_OBJECT):
    #     clist = []
    #     for i in range(self.impl.classCount(package)):
    #         key = self.impl.getClass(package, i)
    #         class_kind = self.impl.getClassKind(key)
    #         if class_kind == kind:
    #             if kind == CLASS_OBJECT:
    #                 clist.append(SchemaObjectClass(None, None, {"impl":self.impl.getObjectClass(key)}))
    #             elif kind == CLASS_EVENT:
    #                 clist.append(SchemaEventClass(None, None, {"impl":self.impl.getEventClass(key)}))
    #     return clist
    
    
    # def bind_package(self, package):
    #     return self.impl.bindPackage(package)
    
    
    # def bind_class(self, kwargs = {}):
    #     if "key" in kwargs:
    #         self.impl.bindClass(kwargs["key"])
    #     elif "package" in kwargs:
    #         package = kwargs["package"]
    #         if "class" in kwargs:
    #             self.impl.bindClass(package, kwargs["class"])
    #         else:
    #             self.impl.bindClass(package)
    #     else:
    #         raise Exception("Argument error: invalid arguments, use 'key' or 'package'[,'class']")
    
    
    # def get_agents(self, broker=None):
    #     blist = []
    #     if broker:
    #         blist.append(broker)
    #     else:
    #         self._cv.acquire()
    #         try:
    #             # copy while holding lock
    #             blist = self._broker_list[:]
    #         finally:
    #             self._cv.release()

    #     agents = []
    #     for b in blist:
    #         for idx in range(b.impl.agentCount()):
    #             agents.append(AgentProxy(b.impl.getAgent(idx), b))

    #     return agents
    
    
    # def get_objects(self, query, kwargs = {}):
    #     timeout = 30
    #     agent = None
    #     temp_args = kwargs.copy()
    #     if type(query) == type({}):
    #         temp_args.update(query)

    #     if "_timeout" in temp_args:
    #         timeout = temp_args["_timeout"]
    #         temp_args.pop("_timeout")

    #     if "_agent" in temp_args:
    #         agent = temp_args["_agent"]
    #         temp_args.pop("_agent")

    #     if type(query) == type({}):
    #         query = Query(temp_args)

    #     self._select = {}
    #     for k in temp_args.iterkeys():
    #         if type(k) == str:
    #             self._select[k] = temp_args[k]

    #     self._cv.acquire()
    #     try:
    #         self._sync_count = 1
    #         self._sync_result = []
    #         broker = self._broker_list[0]
    #         broker.send_query(query.impl, None, agent)
    #         self._cv.wait(timeout)
    #         if self._sync_count == 1:
    #             raise Exception("Timed out: waiting for query response")
    #     finally:
    #         self._cv.release()

    #     return self._sync_result
    
    
    # def get_object(self, query, kwargs = {}):
    #     '''
    #     Return one and only one object or None.
    #     '''
    #     objs = objects(query, kwargs)
    #     if len(objs) == 1:
    #         return objs[0]
    #     else:
    #         return None


    # def first_object(self, query, kwargs = {}):
    #     '''
    #     Return the first of potentially many objects.
    #     '''
    #     objs = objects(query, kwargs)
    #     if objs:
    #         return objs[0]
    #     else:
    #         return None


    # # Check the object against select to check for a match
    # def _select_match(self, object):
    #     schema_props = object.properties()
    #     for key in self._select.iterkeys():
    #         for prop in schema_props:
    #             if key == p[0].name() and self._select[key] != p[1]:
    #                 return False
    #     return True


    # def _get_result(self, list, context):
    #     '''
    #     Called by Broker proxy to return the result of a query.
    #     '''
    #     self._cv.acquire()
    #     try:
    #         for item in list:
    #             if self._select_match(item):
    #                 self._sync_result.append(item)
    #         self._sync_count -= 1
    #         self._cv.notify()
    #     finally:
    #         self._cv.release()


    # def start_sync(self, query): pass
    
    
    # def touch_sync(self, sync): pass
    
    
    # def end_sync(self, sync): pass
    
    


#     def start_console_events(self):
#         self._cb_cond.acquire()
#         try:
#             self._cb_cond.notify()
#         finally:
#             self._cb_cond.release()


#     def _do_console_events(self):
#         '''
#         Called by the Console thread to poll for events.  Passes the events
#         onto the ConsoleHandler associated with this Console.  Is called
#         periodically, but can also be kicked by Console.start_console_events().
#         '''
#         count = 0
#         valid = self.impl.getEvent(self._event)
#         while valid:
#             count += 1
#             try:
#                 if self._event.kind == qmfengine.ConsoleEvent.AGENT_ADDED:
#                     trace.debug("Console Event AGENT_ADDED received")
#                     if self._handler:
#                         self._handler.agent_added(AgentProxy(self._event.agent, None))
#                 elif self._event.kind == qmfengine.ConsoleEvent.AGENT_DELETED:
#                     trace.debug("Console Event AGENT_DELETED received")
#                     if self._handler:
#                         self._handler.agent_deleted(AgentProxy(self._event.agent, None))
#                 elif self._event.kind == qmfengine.ConsoleEvent.NEW_PACKAGE:
#                     trace.debug("Console Event NEW_PACKAGE received")
#                     if self._handler:
#                         self._handler.new_package(self._event.name)
#                 elif self._event.kind == qmfengine.ConsoleEvent.NEW_CLASS:
#                     trace.debug("Console Event NEW_CLASS received")
#                     if self._handler:
#                         self._handler.new_class(SchemaClassKey(self._event.classKey))
#                 elif self._event.kind == qmfengine.ConsoleEvent.OBJECT_UPDATE:
#                     trace.debug("Console Event OBJECT_UPDATE received")
#                     if self._handler:
#                         self._handler.object_update(ConsoleObject(None, {"impl":self._event.object}),
#                                                     self._event.hasProps, self._event.hasStats)
#                 elif self._event.kind == qmfengine.ConsoleEvent.EVENT_RECEIVED:
#                     trace.debug("Console Event EVENT_RECEIVED received")
#                 elif self._event.kind == qmfengine.ConsoleEvent.AGENT_HEARTBEAT:
#                     trace.debug("Console Event AGENT_HEARTBEAT received")
#                     if self._handler:
#                         self._handler.agent_heartbeat(AgentProxy(self._event.agent, None), self._event.timestamp)
#                 elif self._event.kind == qmfengine.ConsoleEvent.METHOD_RESPONSE:
#                     trace.debug("Console Event METHOD_RESPONSE received")
#                 else:
#                     trace.debug("Console thread received unknown event: '%s'" % str(self._event.kind))
#             except e:
#                 print "Exception caught in callback thread:", e
#             self.impl.popEvent()
#             valid = self.impl.getEvent(self._event)
#         return count





# class Broker(ConnectionHandler):
#     #   attr_reader :impl :conn, :console, :broker_bank
#     def __init__(self, console, conn):
#         self.broker_bank = 1
#         self.console = console
#         self.conn = conn
#         self._session = None
#         self._cv = Condition()
#         self._stable = None
#         self._event = qmfengine.BrokerEvent()
#         self._xmtMessage = qmfengine.Message()
#         self.impl = qmfengine.BrokerProxy(self.console.impl)
#         self.console.impl.addConnection(self.impl, self)
#         self.conn.add_conn_handler(self)
#         self._operational = True
    
    
#     def shutdown(self):
#         trace.debug("broker.shutdown() called.")
#         self.console.impl.delConnection(self.impl)
#         self.conn.del_conn_handler(self)
#         if self._session:
#             self.impl.sessionClosed()
#             trace.debug("broker.shutdown() sessionClosed done.")
#             self._session.destroy()
#             trace.debug("broker.shutdown() session destroy done.")
#             self._session = None
#         self._operational = False
#         trace.debug("broker.shutdown() done.")


#     def wait_for_stable(self, timeout = None):
#         self._cv.acquire()
#         try:
#             if self._stable:
#                 return
#             if timeout:
#                 self._cv.wait(timeout)
#                 if not self._stable:
#                     raise Exception("Timed out: waiting for broker connection to become stable")
#             else:
#                 while not self._stable:
#                     self._cv.wait()
#         finally:
#             self._cv.release()


#     def send_query(self, query, ctx, agent):
#         agent_impl = None
#         if agent:
#             agent_impl = agent.impl
#         self.impl.sendQuery(query, ctx, agent_impl)
#         self.conn.kick()


#     def _do_broker_events(self):
#         count = 0
#         valid = self.impl.getEvent(self._event)
#         while valid:
#             count += 1
#             if self._event.kind == qmfengine.BrokerEvent.BROKER_INFO:
#                 trace.debug("Broker Event BROKER_INFO received");
#             elif self._event.kind == qmfengine.BrokerEvent.DECLARE_QUEUE:
#                 trace.debug("Broker Event DECLARE_QUEUE received");
#                 self.conn.impl.declareQueue(self._session.handle, self._event.name)
#             elif self._event.kind == qmfengine.BrokerEvent.DELETE_QUEUE:
#                 trace.debug("Broker Event DELETE_QUEUE received");
#                 self.conn.impl.deleteQueue(self._session.handle, self._event.name)
#             elif self._event.kind == qmfengine.BrokerEvent.BIND:
#                 trace.debug("Broker Event BIND received");
#                 self.conn.impl.bind(self._session.handle, self._event.exchange, self._event.name, self._event.bindingKey)
#             elif self._event.kind == qmfengine.BrokerEvent.UNBIND:
#                 trace.debug("Broker Event UNBIND received");
#                 self.conn.impl.unbind(self._session.handle, self._event.exchange, self._event.name, self._event.bindingKey)
#             elif self._event.kind == qmfengine.BrokerEvent.SETUP_COMPLETE:
#                 trace.debug("Broker Event SETUP_COMPLETE received");
#                 self.impl.startProtocol()
#             elif self._event.kind == qmfengine.BrokerEvent.STABLE:
#                 trace.debug("Broker Event STABLE received");
#                 self._cv.acquire()
#                 try:
#                     self._stable = True
#                     self._cv.notify()
#                 finally:
#                     self._cv.release()
#             elif self._event.kind == qmfengine.BrokerEvent.QUERY_COMPLETE:
#                 result = []
#                 for idx in range(self._event.queryResponse.getObjectCount()):
#                     result.append(ConsoleObject(None, {"impl":self._event.queryResponse.getObject(idx), "broker":self}))
#                 self.console._get_result(result, self._event.context)
#             elif self._event.kind == qmfengine.BrokerEvent.METHOD_RESPONSE:
#                 obj = self._event.context
#                 obj._method_result(MethodResponse(self._event.methodResponse()))
            
#             self.impl.popEvent()
#             valid = self.impl.getEvent(self._event)
        
#         return count
    
    
#     def _do_broker_messages(self):
#         count = 0
#         valid = self.impl.getXmtMessage(self._xmtMessage)
#         while valid:
#             count += 1
#             trace.debug("Broker: sending msg on connection")
#             self.conn.impl.sendMessage(self._session.handle, self._xmtMessage)
#             self.impl.popXmt()
#             valid = self.impl.getXmtMessage(self._xmtMessage)
        
#         return count
    
    
#     def _do_events(self):
#         while True:
#             self.console.start_console_events()
#             bcnt = self._do_broker_events()
#             mcnt = self._do_broker_messages()
#             if bcnt == 0 and mcnt == 0:
#                 break;
    
    
#     def conn_event_connected(self):
#         trace.debug("Broker: Connection event CONNECTED")
#         self._session = Session(self.conn, "qmfc-%s.%d" % (socket.gethostname(), os.getpid()), self)
#         self.impl.sessionOpened(self._session.handle)
#         self._do_events()
    
    
#     def conn_event_disconnected(self, error):
#         trace.debug("Broker: Connection event DISCONNECTED")
#         pass
    
    
#     def conn_event_visit(self):
#         self._do_events()


#     def sess_event_session_closed(self, context, error):
#         trace.debug("Broker: Session event CLOSED")
#         self.impl.sessionClosed()
    
    
#     def sess_event_recv(self, context, message):
#         trace.debug("Broker: Session event MSG_RECV")
#         if not self._operational:
#             log.warning("Unexpected session event message received by Broker proxy: context='%s'" % str(context))
#         self.impl.handleRcvMessage(message)
#         self._do_events()



################################################################################
################################################################################
################################################################################
################################################################################
#                 TEMPORARY TEST CODE - TO BE DELETED
################################################################################
################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    # temp test code
    import logging
    from common import (qmfTypes, SchemaProperty)

    logging.getLogger().setLevel(logging.INFO)

    logging.info( "************* Creating Async Console **************" )

    class MyNotifier(Notifier):
        def __init__(self, context):
            self._myContext = context
            self.WorkAvailable = False

        def indication(self):
            print("Indication received! context=%d" % self._myContext)
            self.WorkAvailable = True

    _noteMe = MyNotifier( 666 )

    _myConsole = Console(notifier=_noteMe)

    _myConsole.enable_agent_discovery()
    logging.info("Waiting...")


    logging.info( "Destroying console:" )
    _myConsole.destroy( 10 )

    logging.info( "******** Messing around with Schema ********" )

    _sec = SchemaEventClass( _classId=SchemaClassId("myPackage", "myClass",
                                                    stype=SchemaClassId.TYPE_EVENT), 
                             _desc="A typical event schema",
                             _props={"Argument-1": SchemaProperty(_type_code=qmfTypes.TYPE_UINT8,
                                                                  kwargs = {"min":0,
                                                                            "max":100,
                                                                            "unit":"seconds",
                                                                            "desc":"sleep value"}),
                                     "Argument-2": SchemaProperty(_type_code=qmfTypes.TYPE_LSTR,
                                                                  kwargs={"maxlen":100,
                                                                          "desc":"a string argument"})})
    print("_sec=%s" % _sec.get_class_id())
    print("_sec.gePropertyCount()=%d" % _sec.get_property_count() )
    print("_sec.getProperty('Argument-1`)=%s" % _sec.get_property('Argument-1') )
    print("_sec.getProperty('Argument-2`)=%s" % _sec.get_property('Argument-2') )
    try:
        print("_sec.getProperty('not-found')=%s" % _sec.get_property('not-found') )
    except:
        pass
    print("_sec.getProperties()='%s'" % _sec.get_properties())

    print("Adding another argument")
    _arg3 = SchemaProperty( _type_code=qmfTypes.TYPE_BOOL,
                            kwargs={"dir":"IO",
                                    "desc":"a boolean argument"})
    _sec.add_property('Argument-3', _arg3)
    print("_sec=%s" % _sec.get_class_id())
    print("_sec.getPropertyCount()=%d" % _sec.get_property_count() )
    print("_sec.getProperty('Argument-1')=%s" % _sec.get_property('Argument-1') )
    print("_sec.getProperty('Argument-2')=%s" % _sec.get_property('Argument-2') )
    print("_sec.getProperty('Argument-3')=%s" % _sec.get_property('Argument-3') )

    print("_arg3.mapEncode()='%s'" % _arg3.map_encode() )

    _secmap = _sec.map_encode()
    print("_sec.mapEncode()='%s'" % _secmap )

    _sec2 = SchemaEventClass( _map=_secmap )

    print("_sec=%s" % _sec.get_class_id())
    print("_sec2=%s" % _sec2.get_class_id())

    _soc = SchemaObjectClass( _map = {"_schema_id": {"_package_name": "myOtherPackage",
                                                     "_class_name":   "myOtherClass",
                                                     "_type":         "_data"},
                                      "_desc": "A test data object",
                                      "_values":
                                          {"prop1": {"amqp_type": qmfTypes.TYPE_UINT8,
                                                     "access": "RO",
                                                     "index": True,
                                                     "unit": "degrees"},
                                           "prop2": {"amqp_type": qmfTypes.TYPE_UINT8,
                                                     "access": "RW",
                                                     "index": True,
                                                     "desc": "The Second Property(tm)",
                                                     "unit": "radians"},
                                           "statistics": { "amqp_type": qmfTypes.TYPE_DELTATIME,
                                                           "unit": "seconds",
                                                           "desc": "time until I retire"},
                                           "meth1": {"_desc": "A test method",
                                                     "_arguments":
                                                         {"arg1": {"amqp_type": qmfTypes.TYPE_UINT32,
                                                                   "desc": "an argument 1",
                                                                   "dir":  "I"},
                                                          "arg2": {"amqp_type": qmfTypes.TYPE_BOOL,
                                                                   "dir":  "IO",
                                                                   "desc": "some weird boolean"}}},
                                           "meth2": {"_desc": "A test method",
                                                     "_arguments":
                                                         {"m2arg1": {"amqp_type": qmfTypes.TYPE_UINT32,
                                                                     "desc": "an 'nuther argument",
                                                                     "dir":
                                                                         "I"}}}},
                                      "_subtypes":
                                          {"prop1":"qmfProperty",
                                           "prop2":"qmfProperty",
                                           "statistics":"qmfProperty",
                                           "meth1":"qmfMethod",
                                           "meth2":"qmfMethod"},
                                      "_primary_key_names": ["prop2", "prop1"]})

    print("_soc='%s'" % _soc)

    print("_soc.getPrimaryKeyList='%s'" % _soc.get_id_names())

    print("_soc.getPropertyCount='%d'" % _soc.get_property_count())
    print("_soc.getProperties='%s'" % _soc.get_properties())
    print("_soc.getProperty('prop2')='%s'" % _soc.get_property('prop2'))

    print("_soc.getMethodCount='%d'" % _soc.get_method_count())
    print("_soc.getMethods='%s'" % _soc.get_methods())
    print("_soc.getMethod('meth2')='%s'" % _soc.get_method('meth2'))

    _socmap = _soc.map_encode()
    print("_socmap='%s'" % _socmap)
    _soc2 = SchemaObjectClass( _map=_socmap )
    print("_soc='%s'" % _soc)
    print("_soc2='%s'" % _soc2)

    if _soc2.get_class_id() == _soc.get_class_id():
        print("soc and soc2 are the same schema")


    logging.info( "******** Messing around with ObjectIds ********" )


    qd = QmfData( _values={"prop1":1, "prop2":True, "prop3": {"a":"map"}, "prop4": "astring"} )
    print("qd='%s':" % qd)

    print("prop1=%d prop2=%s prop3=%s prop4=%s" % (qd.prop1, qd.prop2, qd.prop3, qd.prop4))

    print("qd map='%s'" % qd.map_encode())
    print("qd getProperty('prop4')='%s'" % qd.get_value("prop4"))
    qd.set_value("prop4", 4, "A test property called 4")
    print("qd setProperty('prop4', 4)='%s'" % qd.get_value("prop4"))
    qd.prop4 = 9
    print("qd.prop4 = 9 ='%s'" % qd.prop4)
    qd["prop4"] = 11
    print("qd[prop4] = 11 ='%s'" % qd["prop4"])

    print("qd.mapEncode()='%s'" % qd.map_encode())
    _qd2 = QmfData( _map = qd.map_encode() )
    print("_qd2.mapEncode()='%s'" % _qd2.map_encode())

    _qmfDesc1 = QmfConsoleData( {"_values" : {"prop1": 1, "statistics": 666,
                                              "prop2": 0}},
                                agent="some agent name?",
                                _schema = _soc)

    print("_qmfDesc1 map='%s'" % _qmfDesc1.map_encode())

    _qmfDesc1._set_schema( _soc )

    print("_qmfDesc1 prop2 = '%s'" % _qmfDesc1.get_value("prop2"))
    print("_qmfDesc1 primarykey = '%s'" % _qmfDesc1.get_object_id())
    print("_qmfDesc1 classid = '%s'" % _qmfDesc1.get_schema_class_id())


    _qmfDescMap = _qmfDesc1.map_encode()
    print("_qmfDescMap='%s'" % _qmfDescMap)

    _qmfDesc2 = QmfData( _map=_qmfDescMap, _schema=_soc )

    print("_qmfDesc2 map='%s'" % _qmfDesc2.map_encode())
    print("_qmfDesc2 prop2 = '%s'" % _qmfDesc2.get_value("prop2"))
    print("_qmfDesc2 primary key = '%s'" % _qmfDesc2.get_object_id())


    logging.info( "******** Messing around with QmfEvents ********" )


    _qmfevent1 = QmfEvent( _timestamp = 1111,
                           _schema = _sec,
                           _values = {"Argument-1": 77, 
                                      "Argument-3": True,
                                      "Argument-2": "a string"})
    print("_qmfevent1.mapEncode()='%s'" % _qmfevent1.map_encode())
    print("_qmfevent1.getTimestamp()='%s'" % _qmfevent1.get_timestamp())

    _qmfevent1Map = _qmfevent1.map_encode()

    _qmfevent2 = QmfEvent(_map=_qmfevent1Map, _schema=_sec)
    print("_qmfevent2.mapEncode()='%s'" % _qmfevent2.map_encode())


    logging.info( "******** Messing around with Queries ********" )

    _q1 = QmfQuery.create_predicate(QmfQuery.TARGET_AGENT,
                                    [QmfQuery.AND,
                                     [QmfQuery.EQ, "vendor", [QmfQuery.QUOTE, "AVendor"]],
                                     [QmfQuery.EQ, [QmfQuery.QUOTE, "SomeProduct"], "product"],
                                     [QmfQuery.EQ, [QmfQuery.UNQUOTE, "name"], [QmfQuery.QUOTE, "Thingy"]],
                                     [QmfQuery.OR,
                                      [QmfQuery.LE, "temperature", -10],
                                      [QmfQuery.FALSE],
                                      [QmfQuery.EXISTS, "namey"]]])

    print("_q1.mapEncode() = [%s]" % _q1.map_encode())
