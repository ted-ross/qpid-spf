/*
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
 */

#include <Python.h>
#include "qpid/spf/Router.h"
#include "qpid/spf/PythonTypes.h"
#include "qpid/spf/SpfExchange.h"
#include "qpid/types/Variant.h"
#include "qpid/amqp_0_10/Codecs.h"
#include "qpid/broker/DeliverableMessage.h"
#include <qpid/broker/Broker.h>
#include <qpid/broker/Message.h>
#include <qpid/broker/QueueRegistry.h>
#include <qpid/broker/LinkRegistry.h>
#include <qpid/broker/Link.h>
#include "qpid/framing/MessageTransferBody.h"
#include "qpid/framing/FieldTable.h"
#include "qpid/framing/reply_exceptions.h"
#include "qpid/log/Statement.h"
#include "qpid/broker/amqp_0_10/MessageTransfer.h"
#include "qmf/org/apache/qpid/router/Package.h"
#include "qmf/org/apache/qpid/router/Router.h"
#include "qmf/org/apache/qpid/router/ArgsRouterAdd_link.h"
#include "qmf/org/apache/qpid/router/ArgsRouterDel_link.h"
#include "qmf/org/apache/qpid/router/ArgsRouterGet_router_data.h"
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>
#include <sstream>

using namespace std;
using namespace qpid::spf;
using qpid::framing::InvalidArgumentException;
using qpid::sys::Mutex;
using qpid::types::Variant;
namespace _qmf = ::qmf::org::apache::qpid::router;

qpid::sys::Mutex Router::lock;
qpid::broker::Broker* Router::broker;

extern "C" {
    PyObject* log_cb_entry(PyObject* self, PyObject* args);
    PyObject* send_cb_entry(PyObject* self, PyObject* args);
    PyObject* local_bind_cb_entry(PyObject* self, PyObject* args);
    PyObject* local_unbind_cb_entry(PyObject* self, PyObject* args);
    PyObject* remote_bind_cb_entry(PyObject* self, PyObject* args);
    PyObject* remote_unbind_cb_entry(PyObject* self, PyObject* args);
}

//
// Define the Python class that is used to make calls from the Python side to the c++ side.
//
namespace qpid {
namespace spf {

typedef struct {
    PyObject_HEAD
    Router* pRouter;
} Adapter;

static PyMethodDef Adapter_methods[] = {
    {"log",           log_cb_entry,           METH_VARARGS, "Emit a Log Line"},
    {"send",          send_cb_entry,          METH_VARARGS, "Send a Control Message"},
    {"local_bind",    local_bind_cb_entry,    METH_VARARGS, "Bind a Subject for Router Reception"},
    {"local_unbind",  local_unbind_cb_entry,  METH_VARARGS, "Unbind a Subject for Router Reception"},
    {"remote_bind",   remote_bind_cb_entry,   METH_VARARGS, "Bind a Subject to a Next-Hop-Router"},
    {"remote_unbind", remote_unbind_cb_entry, METH_VARARGS, "Unbind a Subject from a Next-Hop-Router"},
    {0, 0, 0, 0}
};

static PyTypeObject AdapterType = {
    PyObject_HEAD_INIT(0)
    0,                         /* ob_size*/
    "adapter.Adapter",         /* tp_name*/
    sizeof(Adapter),           /* tp_basicsize*/
    0,                         /* tp_itemsize*/
    0,                         /* tp_dealloc*/
    0,                         /* tp_print*/
    0,                         /* tp_getattr*/
    0,                         /* tp_setattr*/
    0,                         /* tp_compare*/
    0,                         /* tp_repr*/
    0,                         /* tp_as_number*/
    0,                         /* tp_as_sequence*/
    0,                         /* tp_as_mapping*/
    0,                         /* tp_hash */
    0,                         /* tp_call*/
    0,                         /* tp_str*/
    0,                         /* tp_getattro*/
    0,                         /* tp_setattro*/
    0,                         /* tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,        /* tp_flags*/
    "Router Adapter Class",    /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Adapter_methods,           /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
    0                          /* tp_version_tag */
};

static PyMethodDef empty_methods[] = {
  {0, 0, 0, 0}
};

}
}


Router::Router(const string& n, SpfExchange& e, const string& module, const qpid::framing::FieldTable& args) :
    name(n), exchange(e), pyRouter(0), firstInvocation(true), bindingsChanged(false)
{
    PyObject* pName;
    PyObject* pId;
    PyObject* pArea;
    PyObject* pModule;
    PyObject* pClass;
    PyObject* pArgs;

    string routerId(args.getAsString("spf.router_id"));
    string area(args.getAsString("spf.area"));
    int tupleCount(2);
    if (!routerId.empty())
        tupleCount++;
    if (!area.empty())
        tupleCount++;

    {
        Mutex::ScopedLock l(lock);

        pName = PyString_FromString(module.c_str());
        pModule = PyImport_Import(pName);
        Py_DECREF(pName);

        if (pModule) {
            pClass = PyObject_GetAttrString(pModule, "RouterEngine");
            if (pClass && PyClass_Check(pClass)) {
                // import adapter module
                PyObject* adapterModuleName = PyString_FromString("adapter");
                PyObject* adapterModule = PyImport_Import(adapterModuleName);
                Py_DECREF(adapterModuleName);
                if (adapterModule) {
                  // lookup Adapter type
                  PyObject* adapterType = PyObject_GetAttrString(adapterModule, "Adapter");

                  // instantiate adapter instance
                  PyObject* adapterInstance = PyObject_CallObject(adapterType, 0);
                  if (adapterInstance == 0) {
                      PyErr_Print();
                      throw InvalidArgumentException(QPID_MSG("Adapter class cannot be instantiated"));
                  }

                  // Add "this" pointer to the python object
                  ((Adapter*) adapterInstance)->pRouter = this;

                  // ctor args for RouterEngine
                  pArgs = PyTuple_New(tupleCount);

                  // arg 0: adapter instance
                  PyTuple_SetItem(pArgs, 0, adapterInstance);

                  // arg 1: router domain name
                  pName = PyString_FromString(name.c_str());
                  PyTuple_SetItem(pArgs, 1, pName);

                  // arg 2: router id
                  if (!routerId.empty()) {
                      pId = PyString_FromString(routerId.c_str());
                      PyTuple_SetItem(pArgs, 2, pId);
                  }

                  // arg 3: area
                  if (!area.empty()) {
                      pArea = PyString_FromString(area.c_str());
                      PyTuple_SetItem(pArgs, 3, pArea);
                  }

                  // instantate router
                  pyRouter = PyInstance_New(pClass, pArgs, 0);
                  Py_DECREF(pArgs);
                  Py_DECREF(adapterType);
                  Py_DECREF(adapterModule);

                  // no need to DECREF pName or adapterInstance because PyTuple_SetItems steals the refs

                  if (pyRouter == 0) {
                      PyErr_Print();
                      throw InvalidArgumentException(QPID_MSG("RouterEngine class cannot be instantiated"));
                  }
                } else {
                  PyErr_Print();
                  throw InvalidArgumentException(QPID_MSG("Unable to locate adapter module"));
                }
            } else
                throw InvalidArgumentException(QPID_MSG("SPF Routing Module has no RouterEngine class"));
        } else {
            QPID_LOG_CAT(error, routing, "SPF: Routing Module could not be loaded: " << module);
            throw InvalidArgumentException(QPID_MSG("SPF Routing Module could not be loaded: " << module));
        }
    }

    string id(getId());
    remoteQueueName = "spf_" + name + "_" + id;
    unroutableExchangeName = name + "_unroutable";

    timer.add(new Tick(*this, 1));
    timer.start();

    if (broker->getManagementAgent()) {
        mgmtObject.reset(new _qmf::Router(broker->getManagementAgent(), this, name));
        broker->getManagementAgent()->addObject(mgmtObject);
    }
}


Router::~Router()
{
    timer.stop();
    if (mgmtObject.get())
        mgmtObject->resourceDestroy();

    Mutex::ScopedLock l(lock);
    Py_DECREF(pyRouter);
}


bool Router::validateBindingKey(const std::string& key)
{
    PyObject* pName;
    PyObject* pArgs;
    PyObject* pValue;
    PyObject* pValidate;
    bool result = false;

    {
        Mutex::ScopedLock l(lock);
        pValidate = PyObject_GetAttrString(pyRouter, "validateTopicKey");
        if (!pValidate || !PyCallable_Check(pValidate))
            throw InvalidArgumentException(QPID_MSG("RouterEngine class has no validateTopicKey method"));

        pName = PyString_FromString(key.c_str());
        pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, pName);
        pValue = PyObject_CallObject(pValidate, pArgs);
        Py_DECREF(pArgs);
        if (pValue && PyBool_Check(pValue) && PyInt_AS_LONG(pValue))
            result = true;

        if (pValue) {
            Py_DECREF(pValue);
        }
        Py_DECREF(pValidate);
    }

    return result;
}


void Router::bindingAdded(const std::string& key)
{
    PyObject* pName;
    PyObject* pArgs;
    PyObject* pValue;
    PyObject* pMethod;

    {
        Mutex::ScopedLock l(lock);
        pMethod = PyObject_GetAttrString(pyRouter, "addLocalAddress");
        if (!pMethod || !PyCallable_Check(pMethod))
            throw InvalidArgumentException(QPID_MSG("RouterEngine class has no addLocalAddress method"));

        pName = PyString_FromString(key.c_str());
        pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, pName);
        pValue = PyObject_CallObject(pMethod, pArgs);
        Py_DECREF(pArgs);
        if (pValue) {
            Py_DECREF(pValue);
        }
        Py_DECREF(pMethod);
    }
}


string Router::getId()
{
    PyObject* pArgs;
    PyObject* pValue;
    PyObject* pMethod;
    string result;

    {
        Mutex::ScopedLock l(lock);
        pMethod = PyObject_GetAttrString(pyRouter, "getId");
        if (!pMethod || !PyCallable_Check(pMethod))
            throw InvalidArgumentException(QPID_MSG("RouterEngine class has no getId method"));

        pArgs = PyTuple_New(0);
        pValue = PyObject_CallObject(pMethod, pArgs);
        Py_DECREF(pArgs);
        if (pValue) {
            result = PyString_AS_STRING(pValue);
            Py_DECREF(pValue);
        }
        Py_DECREF(pMethod);
    }

    return result;
}


void Router::bindingDeleted(const std::string& key)
{
    PyObject* pName;
    PyObject* pArgs;
    PyObject* pValue;
    PyObject* pMethod;

    {
        Mutex::ScopedLock l(lock);
        pMethod = PyObject_GetAttrString(pyRouter, "delLocalAddress");
        if (!pMethod || !PyCallable_Check(pMethod))
            throw InvalidArgumentException(QPID_MSG("RouterEngine class has no delLocalAddress method"));

        pName = PyString_FromString(key.c_str());
        pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, pName);
        pValue = PyObject_CallObject(pMethod, pArgs);
        Py_DECREF(pArgs);
        if (pValue) {
            Py_DECREF(pValue);
        }
        Py_DECREF(pMethod);
    }
}


void Router::handleControlMessage(qpid::broker::Deliverable& deliverable)
{
    const qpid::broker::Message& msg(deliverable.getMessage());
    string opcode = msg.getPropertyAsString("spf.opcode");

    //
    // If there is no opcode, there's no point in going on.
    //
    if (opcode.empty())
        return;

    string bodyString(msg.getContent());

    // Decode the buffer contents into a Variant::Map
    qpid::amqp_0_10::MapCodec codec;
    Variant::Map bodyMap;
    codec.decode(bodyString, bodyMap);

    {
        //
        // Under lock protection, invoke the Python message handler
        //
        Mutex::ScopedLock l(lock);

        // Convert the Variant::Map into a PyObject
        PythonVariant pv_body(bodyMap);
        PyObject* bodyObject = pv_body.asPyObject();

        PyObject* pOpcode;
        PyObject* pArgs;
        PyObject* pMethod;
        PyObject* pValue;

        pMethod = PyObject_GetAttrString(pyRouter, "handleControlMessage");
        if (!pMethod || !PyCallable_Check(pMethod))
            throw InvalidArgumentException(QPID_MSG("RouterEngine class has no handleControlMessage method"));

        pOpcode = PyString_FromString(opcode.c_str());
        pArgs = PyTuple_New(2);
        PyTuple_SetItem(pArgs, 0, pOpcode);
        PyTuple_SetItem(pArgs, 1, bodyObject);
        pValue = PyObject_CallObject(pMethod, pArgs);
        if(PyErr_Occurred()) {
            PyErr_Print();
        }
        Py_DECREF(pArgs);
        if (pValue) {
            Py_DECREF(pValue);
        }
        Py_DECREF(pMethod);
    }
}


void Router::ModuleInitialize(qpid::broker::Broker* _b)
{
    broker = _b;

    //
    // Setup the QMF schema for this module
    //
    if (broker->getManagementAgent())
        _qmf::Package package(broker->getManagementAgent());

    //
    // Initialize the Python interpreter
    //
    Py_Initialize();

    AdapterType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&AdapterType) < 0) {
        PyErr_Print();
        QPID_LOG_CAT(error, routing, "SPF: Unable to initialize AdapterType for SPF Routing Engine; plugin will not be enabled");
    } else {
        PyObject* m = Py_InitModule3("adapter", empty_methods, "Adapter module");

        // PyModule_AddObject steals the reference to AdapterType
        Py_INCREF(&AdapterType);
        PyModule_AddObject(m, "Adapter", (PyObject*)&AdapterType);
    }
}


void Router::ModuleFinalize()
{
    Py_Finalize();
}


PyObject* log_cb_entry(PyObject* self, PyObject* args)           { return ((Adapter*) self)->pRouter->log_cb(args);           }
PyObject* send_cb_entry(PyObject* self, PyObject* args)          { return ((Adapter*) self)->pRouter->send_cb(args);          }
PyObject* local_bind_cb_entry(PyObject* self, PyObject* args)    { return ((Adapter*) self)->pRouter->local_bind_cb(args);    }
PyObject* local_unbind_cb_entry(PyObject* self, PyObject* args)  { return ((Adapter*) self)->pRouter->local_unbind_cb(args);  }
PyObject* remote_bind_cb_entry(PyObject* self, PyObject* args)   { return ((Adapter*) self)->pRouter->remote_bind_cb(args);   }
PyObject* remote_unbind_cb_entry(PyObject* self, PyObject* args) { return ((Adapter*) self)->pRouter->remote_unbind_cb(args); }


PyObject* Router::log_cb(PyObject* args)
{
    int level;
    const char* text;

    if (!PyArg_ParseTuple(args, "is", &level, &text))
        return 0;

    stringstream msg;
    msg << "SPF: Router (py) domain:" << name << " - " << text;

    switch (level) {
    case qpid::log::trace    : QPID_LOG_CAT(trace,    routing, msg.str()); break;
    case qpid::log::debug    : QPID_LOG_CAT(debug,    routing, msg.str()); break;
    case qpid::log::info     : QPID_LOG_CAT(info,     routing, msg.str()); break;
    case qpid::log::notice   : QPID_LOG_CAT(notice,   routing, msg.str()); break;
    case qpid::log::warning  : QPID_LOG_CAT(warning,  routing, msg.str()); break;
    case qpid::log::error    : QPID_LOG_CAT(error,    routing, msg.str()); break;
    case qpid::log::critical : QPID_LOG_CAT(critical, routing, msg.str()); break;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


PyObject* Router::send_cb(PyObject* args)
{
    const char* dest;
    const char* opcode;
    PyObject* body;

    if (!PyArg_ParseTuple(args, "ssO!", &dest, &opcode, &PyDict_Type, &body))
        return 0;

    //
    // Convert the body to a Variant
    //
    PythonVariant pv_body(body);
    const Variant& v_body(pv_body.asVariant());
    Py_DECREF(body);

    //
    // Encode the Variant into the body of a new message
    //
    qpid::amqp_0_10::MapCodec codec;
    string encoded;
    codec.encode(v_body.asMap(), encoded);

    //
    // Route the message to the appropriate destination
    //
    boost::intrusive_ptr<qpid::broker::amqp_0_10::MessageTransfer> msg(new qpid::broker::amqp_0_10::MessageTransfer());
    qpid::framing::AMQFrame method((qpid::framing::MessageTransferBody(qpid::framing::ProtocolVersion(), name, 0, 0)));
    qpid::framing::AMQFrame header((qpid::framing::AMQHeaderBody()));
    qpid::framing::AMQFrame content((qpid::framing::AMQContentBody(encoded)));

    method.setEof(false);
    header.setBof(false);
    header.setEof(false);
    content.setBof(false);

    msg->getFrames().append(method);
    msg->getFrames().append(header);
 
    qpid::framing::MessageProperties* props =
        msg->getFrames().getHeaders()->get<qpid::framing::MessageProperties>(true);

    props->setContentLength(encoded.length());
    //if (!cid.empty()) {
    //    props->setCorrelationId(cid);
    //}
    props->setContentType("amqp/map");
    props->getApplicationHeaders().setString("spf.opcode", opcode);

    qpid::framing::DeliveryProperties* dp =
        msg->getFrames().getHeaders()->get<qpid::framing::DeliveryProperties>(true);
    dp->setRoutingKey(dest);
    //if (ttl_msec) {
    //    dp->setTtl(ttl_msec);
    //    msg->computeExpiration(broker->getExpiryPolicy());
    //}
    msg->getFrames().append(content);

    qpid::broker::Message bmsg(msg, msg);
    bmsg.setIsManagementMessage(true);
    qpid::broker::DeliverableMessage deliverable(bmsg, 0);
    try {
        exchange.routeOutbound(deliverable);
    } catch(exception&) {}

    Py_INCREF(Py_None);
    return Py_None;
}


PyObject* Router::local_bind_cb(PyObject* args)
{
    const char* subject;

    if (!PyArg_ParseTuple(args, "s", &subject))
        return 0;

    exchange.localBind(subject);
    bindingsChanged = true;
    QPID_LOG_CAT(debug, routing, "SPF: Added Local Binding: domain=" << name << " subject=" << subject);

    Py_INCREF(Py_None);
    return Py_None;
}


PyObject* Router::local_unbind_cb(PyObject* args)
{
    const char* subject;

    if (!PyArg_ParseTuple(args, "s", &subject))
        return 0;

    exchange.localUnbind(subject);
    bindingsChanged = true;
    QPID_LOG_CAT(debug, routing, "SPF: Deleted Local Binding: domain=" << name << " subject=" << subject);

    Py_INCREF(Py_None);
    return Py_None;
}


PyObject* Router::remote_bind_cb(PyObject* args)
{
    const char* subject;
    const char* peer_id;

    if (!PyArg_ParseTuple(args, "ss", &subject, &peer_id))
        return 0;

    stringstream queueName;
    queueName << "spf_" << name << "_" << peer_id;
    qpid::broker::Queue::shared_ptr queue(exchange.getBroker()->getQueues().find(queueName.str()));
    if (queue.get()) {
        queue->bind(exchange, subject, qpid::framing::FieldTable());
        bindingsChanged = true;
        QPID_LOG_CAT(debug, routing, "SPF: Added Remote Binding: domain=" << name << " subject=" << subject << " peer_id=" << peer_id);
    }

    Py_INCREF(Py_None);
    return Py_None;
}


PyObject* Router::remote_unbind_cb(PyObject* args)
{
    const char* subject;
    const char* peer_id;

    if (!PyArg_ParseTuple(args, "ss", &subject, &peer_id))
        return 0;

    stringstream queueName;
    queueName << "spf_" << name << "_" << peer_id;
    QPID_LOG_CAT(debug, routing, "SPF: Looking for queue " << queueName.str() << " to delete remote binding");
    qpid::broker::Queue::shared_ptr queue(exchange.getBroker()->getQueues().find(queueName.str()));
    if (queue.get()) {
        exchange.unbind(queue, subject, 0);
        bindingsChanged = true;
        QPID_LOG_CAT(debug, routing, "SPF: Deleted Remote Binding: domain=" << name << " subject=" << subject << "peer_id=" << peer_id);
    } else {
      QPID_LOG_CAT(debug, routing, "SPF: Unable to find queue " << queueName.str() << " to delete remote binding");
    }

    Py_INCREF(Py_None);
    return Py_None;
}


qpid::management::Manageable::status_t Router::ManagementMethod(uint32_t methodId, qpid::management::Args& args, std::string& /*text*/)
{
    qpid::broker::LinkRegistry& links(broker->getLinks());

    switch (methodId) {
    case _qmf::Router::METHOD_ADD_LINK: {
        _qmf::ArgsRouterAdd_link& al(dynamic_cast<_qmf::ArgsRouterAdd_link&>(args));
        string label("link-" + al.i_host + ":" + boost::lexical_cast<string, uint16_t>(al.i_port));
        std::pair<boost::shared_ptr<qpid::broker::Link>, bool> result;
        result = links.declare(label, al.i_host, al.i_port, al.i_transport, false, al.i_authMechanism, al.i_username, al.i_password);
        if (!result.first)
            return qpid::management::Manageable::STATUS_USER;
        links.declare(label, *(result.first), false, name, name, "_peer", false, false, "", "", false, 0, 0, remoteQueueName, unroutableExchangeName);
        return qpid::management::Manageable::STATUS_OK;
    }

    case _qmf::Router::METHOD_DEL_LINK: {
        _qmf::ArgsRouterDel_link& dl(dynamic_cast<_qmf::ArgsRouterDel_link&>(args));
        string label("link-" + dl.i_host + ":" + boost::lexical_cast<string, uint16_t>(dl.i_port));
        boost::shared_ptr<qpid::broker::Link> link(links.getLink(label));
        if (link)
            link->close();
        return qpid::management::Manageable::STATUS_OK;
    }

    case _qmf::Router::METHOD_GET_ROUTER_DATA: {
        _qmf::ArgsRouterGet_router_data& gd(dynamic_cast<_qmf::ArgsRouterGet_router_data&>(args));
        getRouterData(gd.i_kind, gd.o_result);
        return qpid::management::Manageable::STATUS_OK;
    }
    }
    return qpid::management::Manageable::STATUS_UNKNOWN_METHOD;
}


void Router::getRouterData(const std::string& kind, qpid::types::Variant::Map& result)
{
    Mutex::ScopedLock l(lock);

    PyObject* pKind;
    PyObject* pArgs;
    PyObject* pMethod;
    PyObject* pValue;

    pMethod = PyObject_GetAttrString(pyRouter, "getRouterData");
    if (!pMethod || !PyCallable_Check(pMethod))
        throw InvalidArgumentException(QPID_MSG("RouterEngine class has no getRouterData method"));

    pKind = PyString_FromString(kind.c_str());
    pArgs = PyTuple_New(1);
    PyTuple_SetItem(pArgs, 0, pKind);
    pValue = PyObject_CallObject(pMethod, pArgs);
    if(PyErr_Occurred()) {
        PyErr_Print();
    }
    Py_DECREF(pArgs);
    if (pValue) {
        if (PyDict_Check(pValue)) {
            PythonVariant pv(pValue);
            const Variant& v(pv.asVariant());
            result = v.asMap();
        }
        Py_DECREF(pValue);
    }
    Py_DECREF(pMethod);
}


void Router::tick()
{
    PyObject* pValue;
    PyObject* pTick;
    PyObject* pArgs;
    bool reprocess(false);

    if (firstInvocation) {
        firstInvocation = false;
        exchange.setupRouter(name);
    }

    {
        Mutex::ScopedLock l(lock);
        pTick = PyObject_GetAttrString(pyRouter, "handleTimerTick");
        if (!pTick || !PyCallable_Check(pTick))
            throw InvalidArgumentException(QPID_MSG("RouterEngine class has no handleTimerTick method"));

        pArgs = PyTuple_New(0);
        pValue = PyObject_CallObject(pTick, pArgs);
        if(PyErr_Occurred()) {
          PyErr_Print();
        }

        Py_DECREF(pArgs);
        if (pValue) {
            Py_DECREF(pValue);
        }
        Py_DECREF(pTick);
        if (bindingsChanged)
            reprocess = true;
        bindingsChanged = false;
    }

    if (reprocess) {
        QPID_LOG_CAT(debug, routing, "SPF: Bindings changed during router-tick.  Reprocessing held messages");
        exchange.reprocessHeldMessages();
    }
}


Router::Tick::Tick(Router& _r, uint32_t interval) :
    TimerTask(sys::Duration((interval ? interval : 1) * sys::TIME_SEC), "spf::Router"),
    router(_r)
{
    // Intentionally Left Blank
}


void Router::Tick::fire()
{
    setupNextFire();
    router.timer.add(this);

    router.tick();
}

