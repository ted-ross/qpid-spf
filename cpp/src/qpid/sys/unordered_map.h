#ifndef _sys_unordered_map_h
#define _sys_unordered_map_h

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

// unordered_map include path is platform specific

#ifdef _MSC_VER
#  include <unordered_map>
#elif defined(__SUNPRO_CC)
#  include <boost/tr1/unordered_map.hpp>
#else
#  include <tr1/unordered_map>
#endif /* _MSC_VER */
namespace qpid {
namespace sys {
    using std::tr1::unordered_map;
}}


#endif /* _sys_unordered_map_h */
