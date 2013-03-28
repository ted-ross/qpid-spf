/*
 *
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
 *
 */

#include "qpid/sys/posix/PrivatePosix.h"

#include "qpid/sys/Time.h"
#include <ostream>
#include <time.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <iomanip>

namespace {
int64_t max_abstime() { return std::numeric_limits<int64_t>::max(); }
}

namespace qpid {
namespace sys {

AbsTime::AbsTime(const AbsTime& t, const Duration& d) :
    timepoint(d == Duration::max() ? max_abstime() : t.timepoint+d.nanosecs)
{}

AbsTime AbsTime::Epoch() {
    AbsTime epoch; epoch.timepoint = 0;
    return epoch;
}

AbsTime AbsTime::FarFuture() {
    AbsTime ff; ff.timepoint = max_abstime(); return ff;
}

AbsTime AbsTime::now() {
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    AbsTime time_now;
    time_now.timepoint = toTime(ts).nanosecs;
    return time_now;
}

Duration::Duration(const AbsTime& start, const AbsTime& finish) :
    nanosecs(finish.timepoint - start.timepoint)
{}

namespace {
/** type conversion helper: an infinite timeout for time_t sized types **/
const time_t TIME_T_MAX = std::numeric_limits<time_t>::max();
}

struct timespec& toTimespec(struct timespec& ts, const Duration& t) {
    Duration secs = t / TIME_SEC;
    ts.tv_sec = (secs > TIME_T_MAX) ? TIME_T_MAX : static_cast<time_t>(secs);
    ts.tv_nsec = static_cast<long>(t % TIME_SEC);
    return ts; 
}

struct timeval& toTimeval(struct timeval& tv, const Duration& t) {
    Duration secs = t / TIME_SEC;
    tv.tv_sec = (secs > TIME_T_MAX) ? TIME_T_MAX : static_cast<time_t>(secs);
    tv.tv_usec = static_cast<suseconds_t>((t%TIME_SEC)/TIME_USEC);
    return tv;
}

Duration toTime(const struct timespec& ts) {
    return ts.tv_sec*TIME_SEC + ts.tv_nsec;
}

std::ostream& operator<<(std::ostream& o, const Duration& d) {
    return o << int64_t(d) << "ns";   
}

namespace {
inline std::ostream& outputFormattedTime(std::ostream& o, const ::time_t* time) {
    ::tm timeinfo;
    char time_string[100];
    ::strftime(time_string, 100,
               "%Y-%m-%d %H:%M:%S",
               localtime_r(time, &timeinfo));
    return o << time_string;
}
}

std::ostream& operator<<(std::ostream& o, const AbsTime& t) {
    ::time_t rawtime(t.timepoint/TIME_SEC);
    return outputFormattedTime(o, &rawtime);
}

void outputFormattedNow(std::ostream& o) {
    ::time_t rawtime;
    ::time(&rawtime);
    outputFormattedTime(o, &rawtime);
    o << " ";
}

void outputHiresNow(std::ostream& o) {
    ::timespec time;
    ::clock_gettime(CLOCK_REALTIME, &time);
    ::time_t seconds = time.tv_sec;
    outputFormattedTime(o, &seconds);
    o << "." << std::setw(9) << std::setfill('0') << time.tv_nsec << " ";
}

void sleep(int secs) {
    ::sleep(secs);
}

void usleep(uint64_t usecs) {
    ::usleep(usecs);
}

}}
