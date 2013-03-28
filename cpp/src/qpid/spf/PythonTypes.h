#ifndef _qpid_spf_PythonTypes_h_
#define _qpid_spf_PythonTypes_h_ 1
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
#include <qpid/types/Variant.h>

namespace qpid {
namespace spf {

    class PythonVariant {
    public:
        PythonVariant(PyObject* value);
        PythonVariant(const qpid::types::Variant& value);
        ~PythonVariant();

        PyObject* asPyObject() const { return pyObject; }
        const qpid::types::Variant& asVariant()  const { return variant; }

    private:
        PyObject* pyObject;
        const qpid::types::Variant variant;

        qpid::types::Variant PyToVariant(PyObject* value);
        PyObject* VariantToPy(const qpid::types::Variant& v);
        PyObject* MapToPy(const qpid::types::Variant::Map&);
        PyObject* ListToPy(const qpid::types::Variant::List&);
        void PyToMap(PyObject*, qpid::types::Variant::Map&);
        void PyToList(PyObject*, qpid::types::Variant::List&);
        void PyTupleToList(PyObject*, qpid::types::Variant::List&);
    };

}}

#endif

