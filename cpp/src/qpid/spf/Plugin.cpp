/*
 *
 * Copyright (c) 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <Python.h>
#include <sstream>
#include "qpid/acl/Acl.h"
#include "qpid/broker/Broker.h"
#include "qpid/Plugin.h"
#include "qpid/Options.h"
#include "qpid/log/Statement.h"

#include <boost/shared_ptr.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "qpid/spf/SpfExchange.h"
#include "qpid/spf/Router.h"

using namespace qpid;
using namespace std;
class Broker;

namespace qpid {
namespace spf {

struct SpfOptions : public Options {
    string pythonModule;

    SpfOptions() : Options("Shortest Path Federation Options"), pythonModule("spfengine") {
        addOptions()
            ("spf-module", optValue(pythonModule, "NAME"), "The Python module that supplies the routing function");
    }
};


broker::Exchange::shared_ptr spf_exchange_create(const string& name, bool durable,
                                                 const framing::FieldTable& args, 
                                                 management::Manageable* parent,
                                                 broker::Broker* broker)
{
    broker::Exchange::shared_ptr e(new spf::SpfExchange(name, durable, args, parent, broker));
    return e;
}


class SpfExchangePlugin : public Plugin
{
public:
    SpfOptions options;
    void earlyInitialize(Plugin::Target& target);
    void initialize(Plugin::Target& target);
    void finalize();
    Options* getOptions() { return &options; }
    const string getName() const { return "SPF Exchange"; }
};


void SpfExchangePlugin::earlyInitialize(Plugin::Target& target)
{
    broker::Broker* broker = dynamic_cast<broker::Broker*>(&target);
    if (broker) {
        broker->getExchanges().registerType(spf::SpfExchange::typeName, &spf_exchange_create);
        QPID_LOG(notice, "Shortest-Path-Federation Exchange Loaded");
    }
}


void SpfExchangePlugin::initialize(Target& target)
{
    broker::Broker* broker = dynamic_cast<broker::Broker*>(&target);

    if (broker) {
        broker->addFinalizer(boost::bind(&SpfExchangePlugin::finalize, this));
        Router::ModuleInitialize(broker);
    }
}


void SpfExchangePlugin::finalize()
{
    //
    // Finalize the Python interpreter
    //
    Router::ModuleFinalize();
}


static SpfExchangePlugin spfPlugin;

}}
