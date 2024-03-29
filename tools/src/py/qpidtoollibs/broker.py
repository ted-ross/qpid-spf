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

from qpid.messaging import Message
from qpidtoollibs.disp import TimeLong
try:
  from uuid import uuid4
except ImportError:
  from qpid.datatypes import uuid4

class BrokerAgent(object):
  """
  Proxy for a manageable Qpid Broker - Invoke with an opened qpid.messaging.Connection.
  """
  def __init__(self, conn):
    self.conn = conn
    self.sess = self.conn.session()
    self.reply_to = "qmf.default.topic/direct.%s;{node:{type:topic}, link:{x-declare:{auto-delete:True,exclusive:True}}}" % \
        str(uuid4())
    self.reply_rx = self.sess.receiver(self.reply_to)
    self.reply_rx.capacity = 10
    self.tx = self.sess.sender("qmf.default.direct/broker")
    self.next_correlator = 1

  def close(self):
    """
    Close the proxy session.  This will not affect the connection used in creating the object.
    """
    self.sess.close()

  def _method(self, method, arguments, addr="org.apache.qpid.broker:broker:amqp-broker", timeout=10):
    props = {'method'             : 'request',
             'qmf.opcode'         : '_method_request',
             'x-amqp-0-10.app-id' : 'qmf2'}
    correlator = str(self.next_correlator)
    self.next_correlator += 1

    content = {'_object_id'   : {'_object_name' : addr},
               '_method_name' : method,
               '_arguments'   : arguments}

    message = Message(content, reply_to=self.reply_to, correlation_id=correlator,
                      properties=props, subject="broker")
    self.tx.send(message)
    response = self.reply_rx.fetch(timeout)
    self.sess.acknowledge()
    if response.properties['qmf.opcode'] == '_exception':
      raise Exception("Exception from Agent: %r" % response.content['_values'])
    if response.properties['qmf.opcode'] != '_method_response':
      raise Exception("bad response: %r" % response.properties)
    return response.content['_arguments']

  def _sendRequest(self, opcode, content):
    props = {'method'             : 'request',
             'qmf.opcode'         : opcode,
             'x-amqp-0-10.app-id' : 'qmf2'}
    correlator = str(self.next_correlator)
    self.next_correlator += 1
    message = Message(content, reply_to=self.reply_to, correlation_id=correlator,
                      properties=props, subject="broker")
    self.tx.send(message)
    return correlator

  def _doClassQuery(self, class_name):
    query = {'_what'      : 'OBJECT',
             '_schema_id' : {'_class_name' : class_name}}
    correlator = self._sendRequest('_query_request', query)
    response = self.reply_rx.fetch(10)
    if response.properties['qmf.opcode'] != '_query_response':
      raise Exception("bad response")
    items = []
    done = False
    while not done:
      for item in response.content:
        items.append(item)
      if 'partial' in response.properties:
        response = self.reply_rx.fetch(10)
      else:
        done = True
      self.sess.acknowledge()
    return items

  def _doNameQuery(self, object_id):
    query = {'_what'      : 'OBJECT', '_object_id' : {'_object_name' : object_id}}
    correlator = self._sendRequest('_query_request', query)
    response = self.reply_rx.fetch(10)
    if response.properties['qmf.opcode'] != '_query_response':
      raise Exception("bad response")
    items = []
    done = False
    while not done:
      for item in response.content:
        items.append(item)
      if 'partial' in response.properties:
        response = self.reply_rx.fetch(10)
      else:
        done = True
      self.sess.acknowledge()
    if len(items) == 1:
      return items[0]
    return None

  def _getAllBrokerObjects(self, cls):
    items = self._doClassQuery(cls.__name__.lower())
    objs = []
    for item in items:
      objs.append(cls(self, item))
    return objs

  def _getBrokerObject(self, cls, oid):
    obj = self._doNameQuery(oid)
    if obj:
      return cls(self, obj)
    return None

  def _getSingleObject(self, cls):
    #
    # getAllBrokerObjects is used instead of getBrokerObject(Broker, 'amqp-broker') because
    # of a bug that used to be in the broker whereby by-name queries did not return the
    # object timestamps.
    #
    objects = self._getAllBrokerObjects(cls)
    if objects: return objects[0]
    return None

  def getBroker(self):
    """
    Get the Broker object that contains broker-scope statistics and operations.
    """
    return self._getSingleObject(Broker)


  def getCluster(self):
    return self._getSingleObject(Cluster)

  def getHaBroker(self):
    return self._getSingleObject(HaBroker)

  def getAllConnections(self):
    return self._getAllBrokerObjects(Connection)

  def getConnection(self, oid):
    return self._getBrokerObject(Connection, "org.apache.qpid.broker:connection:%s" % oid)

  def getAllSessions(self):
    return self._getAllBrokerObjects(Session)

  def getSession(self, oid):
    return self._getBrokerObject(Session, "org.apache.qpid.broker:session:%s" % oid)

  def getAllSubscriptions(self):
    return self._getAllBrokerObjects(Subscription)

  def getSubscription(self, oid):
    return self._getBrokerObject(Subscription, "org.apache.qpid.broker:subscription:%s" % oid)

  def getAllExchanges(self):
    return self._getAllBrokerObjects(Exchange)

  def getExchange(self, name):
    return self._getBrokerObject(Exchange, "org.apache.qpid.broker:exchange:%s" % name)

  def getAllQueues(self):
    return self._getAllBrokerObjects(Queue)

  def getQueue(self, name):
    return self._getBrokerObject(Queue, "org.apache.qpid.broker:queue:%s" % name)

  def getAllBindings(self):
    return self._getAllBrokerObjects(Binding)

  def getAllLinks(self):
    return self._getAllBrokerObjects(Link)

  def getAcl(self):
    return self._getSingleObject(Acl)

  def getMemory(self):
    return self._getSingleObject(Memory)

  def getAllRouters(self):
    return self._getAllBrokerObjects(Router)

  def getRouter(self, domain):
    return self._getBrokerObject(Router, "org.apache.qpid.router:router:%s" % domain)

  def echo(self, sequence = 1, body = "Body"):
    """Request a response to test the path to the management broker"""
    args = {'sequence' : sequence, 'body' : body}
    return self._method('echo', args)

  def connect(self, host, port, durable, authMechanism, username, password, transport):
    """Establish a connection to another broker"""
    pass

  def queueMoveMessages(self, srcQueue, destQueue, qty):
    """Move messages from one queue to another"""
    pass

  def setLogLevel(self, level):
    """Set the log level"""
    pass

  def getLogLevel(self):
    """Get the log level"""
    pass

  def setTimestampConfig(self, receive):
    """Set the message timestamping configuration"""
    pass

  def getTimestampConfig(self):
    """Get the message timestamping configuration"""
    pass

  def addExchange(self, exchange_type, name, options={}, **kwargs):
    properties = {}
    properties['exchange-type'] = exchange_type
    for k,v in options.items():
      properties[k] = v
    for k,v in kwargs.items():
      properties[k] = v
    args = {'type':       'exchange',
            'name':        name,
            'properties':  properties,
            'strict':      True}
    self._method('create', args)

  def delExchange(self, name):
    args = {'type': 'exchange', 'name': name}
    self._method('delete', args)

  def addQueue(self, name, options={}, **kwargs):
    properties = options
    for k,v in kwargs.items():
      properties[k] = v
    args = {'type':       'queue',
            'name':        name,
            'properties':  properties,
            'strict':      True}
    self._method('create', args)

  def delQueue(self, name, if_empty=True, if_unused=True):
    options = {'if_empty':  if_empty,
               'if_unused': if_unused}

    args = {'type':        'queue', 
            'name':         name,
            'options':      options}
    self._method('delete', args)

  def bind(self, exchange, queue, key, options={}, **kwargs):
    properties = options
    for k,v in kwargs.items():
      properties[k] = v
    args = {'type':       'binding',
            'name':       "%s/%s/%s" % (exchange, queue, key),
            'properties':  properties,
            'strict':      True}
    self._method('create', args)

  def unbind(self, exchange, queue, key, **kwargs):
    args = {'type':       'binding',
            'name':       "%s/%s/%s" % (exchange, queue, key),
            'strict':      True}
    self._method('delete', args)

  def reloadAclFile(self):
    self._method('reloadACLFile', {}, "org.apache.qpid.acl:acl:org.apache.qpid.broker:broker:amqp-broker")

  def acl_lookup(self, userName, action, aclObj, aclObjName, propMap):
    args = {'userId':      userName,
            'action':      action,
            'object':      aclObj,
            'objectName':  aclObjName,
            'propertyMap': propMap}
    return self._method('Lookup', args, "org.apache.qpid.acl:acl:org.apache.qpid.broker:broker:amqp-broker")

  def acl_lookupPublish(self, userName, exchange, key):
    args = {'userId':       userName,
            'exchangeName': exchange,
            'routingKey':   key}
    return self._method('LookupPublish', args, "org.apache.qpid.acl:acl:org.apache.qpid.broker:broker:amqp-broker")

  def create(self, _type, name, properties={}, strict=False):
    """Create an object of the specified type"""
    args = {'type': _type,
            'name': name,
            'properties': properties,
            'strict': strict}
    return self._method('create', args)

  def delete(self, _type, name, options):
    """Delete an object of the specified type"""
    args = {'type': _type,
            'name': name,
            'options': options}
    return self._method('delete', args)

  def query(self, _type, oid):
    """Query the current state of an object"""
    return self._getBrokerObject(self, _type, oid)


class EventHelper(object):
  def eventAddress(self, pkg='*', cls='*', sev='*'):
    return "qmf.default.topic/agent.ind.event.%s.%s.%s.#" % (pkg.replace('.', '_'), cls, sev)

  def event(self, msg):
    return BrokerEvent(msg)


class BrokerEvent(object):
  def __init__(self, msg):
    self.msg = msg
    self.content = msg.content[0]
    self.values = self.content['_values']
    self.schema_id = self.content['_schema_id']
    self.name = "%s:%s" % (self.schema_id['_package_name'], self.schema_id['_class_name'])

  def __repr__(self):
    rep = "%s %s" % (TimeLong(self.getTimestamp()), self.name)
    for k,v in self.values.items():
      rep = rep + " %s=%s" % (k, v)
    return rep

  def __getattr__(self, key):
    if key not in self.values:
      return None
    value = self.values[key]
    return value

  def getAttributes(self):
    return self.values

  def getTimestamp(self):
    return self.content['_timestamp']


class BrokerObject(object):
  def __init__(self, broker, content):
    self.broker = broker
    self.content = content
    self.values = content['_values']

  def __getattr__(self, key):
    if key not in self.values:
      return None
    value = self.values[key]
    if value.__class__ == dict and '_object_name' in value:
      full_name = value['_object_name']
      colon = full_name.find(':')
      if colon > 0:
        full_name = full_name[colon+1:]
        colon = full_name.find(':')
        if colon > 0:
          return full_name[colon+1:]
    return value

  def getObjectId(self):
    return self.content['_object_id']['_object_name']

  def getAttributes(self):
    return self.values

  def getCreateTime(self):
    return self.content['_create_ts']

  def getDeleteTime(self):
    return self.content['_delete_ts']

  def getUpdateTime(self):
    return self.content['_update_ts']

  def update(self):
    """
    Reload the property values from the agent.
    """
    refreshed = self.broker._getBrokerObject(self.__class__, self.getObjectId())
    if refreshed:
      self.content = refreshed.content
      self.values = self.content['_values']
    else:
      raise Exception("No longer exists on the broker")

class Broker(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Cluster(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class HaBroker(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Memory(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Connection(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

  def close(self):
    self.broker._method("close", {}, "org.apache.qpid.broker:connection:%s" % self.address)

class Session(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Subscription(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

  def __repr__(self):
    return "subscription name undefined"

class Exchange(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Binding(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

  def __repr__(self):
    return "Binding key: %s" % self.values['bindingKey']

class Queue(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

  def purge(self, request):
    """Discard all or some messages on a queue"""
    self.broker._method("purge", {'request':request}, "org.apache.qpid.broker:queue:%s" % self.name)

  def reroute(self, request, useAltExchange, exchange, filter={}):
    """Remove all or some messages on this queue and route them to an exchange"""
    self.broker._method("reroute", {'request':request,'useAltExchange':useAltExchange,'exchange':exchange,'filter':filter},
                        "org.apache.qpid.broker:queue:%s" % self.name)

class Link(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Acl(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

class Router(BrokerObject):
  def __init__(self, broker, values):
    BrokerObject.__init__(self, broker, values)

  def addLink(self, host, port, transport="tcp", authMechanism="ANONYMOUS", username="", password=""):
    self.broker._method("add_link", {'host':host, 'port':port, 'transport':transport, 'authMechanism':authMechanism, 'username':username, 'password':password}, "org.apache.qpid.router:router:%s" % self.domain)

  def delLink(self, host, port):
    self.broker._method("del_link", {'host':host, 'port':port}, "org.apache.qpid.router:router:%s" % self.domain)

  def getRouterData(self, kind):
    result = self.broker._method("get_router_data", {'kind':kind}, "org.apache.qpid.router:router:%s" % self.domain)
    return result['result']

