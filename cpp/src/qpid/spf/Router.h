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

#ifndef _qpid_spf_router_
#define _qpid_spf_router_

#include "qpid/broker/Exchange.h"
#include "qpid/sys/Mutex.h"
#include "qpid/sys/Timer.h"
#include "qpid/management/Manageable.h"
#include "qpid/types/Variant.h"
#include "qmf/org/apache/qpid/router/Router.h"
#include <string>
#include <map>

namespace qpid {
namespace framing { class FieldTable; }
namespace broker { class Deliverable; }
namespace spf {

    class SpfExchange;

    //
    // The Router class is a C++ adapter for the Python implementation of the routing engine.
    //
    class Router : public management::Manageable {
    public:
        Router(const std::string& name, SpfExchange& exchange, const std::string& module, const qpid::framing::FieldTable& args=qpid::framing::FieldTable());
        virtual ~Router();

        std::string getId();
        bool validateBindingKey(const std::string& key);
        void bindingAdded(const std::string& key);
        void bindingDeleted(const std::string& key);
        void handleControlMessage(qpid::broker::Deliverable& msg);

        static void ModuleInitialize(qpid::broker::Broker*);
        static void ModuleFinalize();

        //
        // Methods from qpid::management::Manageable
        //
        qpid::management::ManagementObject::shared_ptr GetManagementObject(void) const { return mgmtObject; }
        qpid::management::Manageable::status_t ManagementMethod(uint32_t methodId, qpid::management::Args& args, std::string& text);

        PyObject* log_cb(PyObject* args);
        PyObject* send_cb(PyObject* args);
        PyObject* local_bind_cb(PyObject* args);
        PyObject* local_unbind_cb(PyObject* args);
        PyObject* remote_bind_cb(PyObject* args);
        PyObject* remote_unbind_cb(PyObject* args);

    private:
        static qpid::sys::Mutex lock;
        static qpid::broker::Broker* broker;

        const std::string name;
        SpfExchange& exchange;
        PyObject* pyRouter;
        qpid::sys::Timer timer;
        std::string remoteQueueName;
        std::string unroutableExchangeName;
        qmf::org::apache::qpid::router::Router::shared_ptr mgmtObject;
        bool firstInvocation;
        bool bindingsChanged;

        void getRouterData(const std::string& kind, qpid::types::Variant::Map& result);
        void tick();

        struct Tick : public qpid::sys::TimerTask {
            Router& router;

            Tick(Router& router, uint32_t interval);
            virtual ~Tick() {}
            void fire();
        };
    };

}
}

#endif
