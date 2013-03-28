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

#include "qpid/spf/PythonTypes.h"
#include <iostream>

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

using namespace std;
using namespace qpid::spf;

PythonVariant::PythonVariant(PyObject* value) : pyObject(value), variant(PyToVariant(value))                   { Py_INCREF(pyObject); }
PythonVariant::PythonVariant(const qpid::types::Variant& value) : pyObject(VariantToPy(value)), variant(value) { Py_INCREF(pyObject); }
PythonVariant::~PythonVariant() { Py_DECREF(pyObject); }


qpid::types::Variant PythonVariant::PyToVariant(PyObject* value)
{
    if (PyBool_Check(value))   return qpid::types::Variant(bool(PyInt_AS_LONG(value) ? true : false));
    if (PyFloat_Check(value))  return qpid::types::Variant(PyFloat_AS_DOUBLE(value));
    if (PyInt_Check(value))    return qpid::types::Variant(int64_t(PyInt_AS_LONG(value)));
    if (PyLong_Check(value))   return qpid::types::Variant(int64_t(PyLong_AsLongLong(value)));
    if (PyString_Check(value)) return qpid::types::Variant(std::string(PyString_AS_STRING(value)));
    if (PyDict_Check(value)) {
        qpid::types::Variant::Map map;
        PyToMap(value, map);
        return qpid::types::Variant(map);
    }
    if (PyList_Check(value)) {
        qpid::types::Variant::List list;
        PyToList(value, list);
        return qpid::types::Variant(list);
    }
    if (PyTuple_Check(value)) {
        qpid::types::Variant::List list;
        PyTupleToList(value, list);
        return qpid::types::Variant(list);
    }
    return qpid::types::Variant();
}


PyObject* PythonVariant::VariantToPy(const qpid::types::Variant& v)
{
    PyObject* result;
    try {
        switch (v.getType()) {
        case qpid::types::VAR_VOID :
        case qpid::types::VAR_UUID : {
            result = Py_None;
            break;
        }
        case qpid::types::VAR_BOOL : {
            result = v.asBool() ? Py_True : Py_False;
            break;
        }
        case qpid::types::VAR_UINT8 :
        case qpid::types::VAR_UINT16 :
        case qpid::types::VAR_UINT32 : {
            result = PyInt_FromLong((long) v.asUint32());
            break;
        }
        case qpid::types::VAR_UINT64 : {
            result = PyLong_FromUnsignedLongLong((unsigned PY_LONG_LONG) v.asUint64());
            break;
        }
        case qpid::types::VAR_INT8 : 
        case qpid::types::VAR_INT16 :
        case qpid::types::VAR_INT32 : {
            result = PyInt_FromLong((long) v.asInt32());
            break;
        }
        case qpid::types::VAR_INT64 : {
            result = PyLong_FromLongLong((PY_LONG_LONG) v.asInt64());
            break;
        }
        case qpid::types::VAR_FLOAT : {
            result = PyFloat_FromDouble((double) v.asFloat());
            break;
        }
        case qpid::types::VAR_DOUBLE : {
            result = PyFloat_FromDouble((double) v.asDouble());
            break;
        }
        case qpid::types::VAR_STRING : {
            const std::string val(v.asString());
            result = PyString_FromStringAndSize(val.c_str(), val.size());
            break;
        }
        case qpid::types::VAR_MAP : {
            result = MapToPy(v.asMap());
            break;
        }
        case qpid::types::VAR_LIST : {
            result = ListToPy(v.asList());
            break;
        }
        }
    } catch (qpid::types::Exception& ex) {
        PyErr_SetString(PyExc_RuntimeError, ex.what());
        result = 0;
    }

    return result;
}


PyObject* PythonVariant::MapToPy(const qpid::types::Variant::Map& map)
{
    PyObject* result = PyDict_New();
    qpid::types::Variant::Map::const_iterator iter;
    for (iter = map.begin(); iter != map.end(); iter++) {
        const std::string key(iter->first);
        PyObject* pyval = VariantToPy(iter->second);
        if (pyval == 0)
            return 0;
        PyObject* pykey = PyString_FromStringAndSize(key.c_str(), key.size());
        PyDict_SetItem(result, pykey, pyval);
        Py_DECREF(pykey);
        Py_DECREF(pyval);
    }
    return result;
}


PyObject* PythonVariant::ListToPy(const qpid::types::Variant::List& list)
{
    PyObject* result = PyList_New(list.size());
    qpid::types::Variant::List::const_iterator iter;
    Py_ssize_t idx(0);
    for (iter = list.begin(); iter != list.end(); iter++) {
        PyObject* pyval = VariantToPy(*iter);
        if (pyval == 0)
            return 0;
        PyList_SetItem(result, idx, pyval);
        idx++;
    }
    return result;
}


void PythonVariant::PyToMap(PyObject* obj, qpid::types::Variant::Map& map)
{
    map.clear();
    Py_ssize_t iter(0);
    PyObject *key;
    PyObject *val;
    while (PyDict_Next(obj, &iter, &key, &val))
        map[std::string(PyString_AS_STRING(key))] = PyToVariant(val);
}


void PythonVariant::PyToList(PyObject* obj, qpid::types::Variant::List& list)
{
    list.clear();
    Py_ssize_t count(PyList_Size(obj));
    for (Py_ssize_t idx = 0; idx < count; idx++) {
        PyObject* item = PyList_GetItem(obj, idx);
        list.push_back(PyToVariant(item));
    }
}


void PythonVariant::PyTupleToList(PyObject* obj, qpid::types::Variant::List& list)
{
    list.clear();
    Py_ssize_t count(PyTuple_Size(obj));
    for (Py_ssize_t idx = 0; idx < count; idx++) {
        PyObject* item = PyTuple_GetItem(obj, idx);
        list.push_back(PyToVariant(item));
    }
}

