#ifndef QMF_ENGINE_IMPORT_EXPORT_H
#define QMF_ENGINE_IMPORT_EXPORT_H

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

#if defined(WIN32) && !defined(QPID_DECLARE_STATIC)
#  if defined(QMF_EXPORT) || defined (qmfengine_EXPORTS)
#    define QMFE_EXTERN __declspec(dllexport)
#  else
#    define QMFE_EXTERN __declspec(dllimport)
#  endif
#  ifdef _MSC_VER
#    define QMFE_CLASS_EXTERN
#    define QMFE_INLINE_EXTERN QMFE_EXTERN
#  else
#    define QMFE_CLASS_EXTERN QMFE_EXTERN
#    define QMFE_INLINE_EXTERN
#  endif
#else
#  define QMFE_EXTERN
#  define QMFE_CLASS_EXTERN
#  define QMFE_INLINE_EXTERN
#endif

#endif
