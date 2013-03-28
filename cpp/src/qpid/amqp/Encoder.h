#ifndef QPID_AMQP_ENCODER_H
#define QPID_AMQP_ENCODER_H

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
#include "qpid/sys/IntegerTypes.h"
#include "qpid/amqp/Constructor.h"
#include <stddef.h>
#include <string>

namespace qpid {
namespace types {
class Uuid;
}
namespace amqp {
struct CharSequence;
struct Descriptor;

/**
 * Class to help create AMQP encoded data.
 */
class Encoder
{
  public:
    void writeCode(uint8_t);

    void write(bool);
    void write(uint8_t);
    void write(uint16_t);
    void write(uint32_t);
    void write(uint64_t);
    void write(int8_t);
    void write(int16_t);
    void write(int32_t);
    void write(int64_t);
    void write(float);
    void write(double);
    void write(const qpid::types::Uuid&);

    void writeNull(const Descriptor* d=0);
    void writeBoolean(bool, const Descriptor* d=0);
    void writeUByte(uint8_t, const Descriptor* d=0);
    void writeUShort(uint16_t, const Descriptor* d=0);
    void writeUInt(uint32_t, const Descriptor* d=0);
    void writeULong(uint64_t, const Descriptor* d=0);
    void writeByte(int8_t, const Descriptor* d=0);
    void writeShort(int16_t, const Descriptor* d=0);
    void writeInt(int32_t, const Descriptor* d=0);
    void writeLong(int64_t, const Descriptor* d=0);
    void writeFloat(float, const Descriptor* d=0);
    void writeDouble(double, const Descriptor* d=0);
    void writeUuid(const qpid::types::Uuid&, const Descriptor* d=0);

    void writeSymbol(const CharSequence&, const Descriptor* d=0);
    void writeSymbol(const std::string&, const Descriptor* d=0);
    void writeString(const CharSequence&, const Descriptor* d=0);
    void writeString(const std::string&, const Descriptor* d=0);
    void writeBinary(const CharSequence&, const Descriptor* d=0);
    void writeBinary(const std::string&, const Descriptor* d=0);

    void* startList8(const Descriptor* d=0);
    void* startList32(const Descriptor* d=0);
    void endList8(uint8_t count, void*);
    void endList32(uint32_t count, void*);

    void* startMap8(const Descriptor* d=0);
    void* startMap32(const Descriptor* d=0);
    void endMap8(uint8_t count, void*);
    void endMap32(uint32_t count, void*);

    void* startArray8(const Constructor&, const Descriptor* d=0);
    void* startArray32(const Constructor&, const Descriptor* d=0);
    void endArray8(size_t count, void*);
    void endArray32(size_t count, void*);

    void writeDescriptor(const Descriptor&);
    Encoder(char* data, size_t size);
    size_t getPosition();
    void resetPosition(size_t p);
    char* skip(size_t);
    void writeBytes(const char* bytes, size_t count);
    virtual ~Encoder() {}
  private:
    char* data;
    size_t size;
    size_t position;

    void write(const CharSequence& v, std::pair<uint8_t, uint8_t> codes, const Descriptor* d);
    void write(const std::string& v, std::pair<uint8_t, uint8_t> codes, const Descriptor* d);
    void check(size_t);

    template<typename T> void write(T value, uint8_t code, const Descriptor* d)
    {
        if (d) writeDescriptor(*d);
        writeCode(code);
        write(value);
    }

    template<typename T> void write(T value, std::pair<uint8_t, uint8_t> codes, const Descriptor* d)
    {
        if (value < 256) {
            write((uint8_t) value, codes.first, d);
        } else {
            write(value, codes.second, d);
        }
    }

    template<typename T> void* start(uint8_t code, const Descriptor* d)
    {
        if (d) writeDescriptor(*d);
        writeCode(code);
        //skip size and count, will backfill on end
        return skip(sizeof(T)/*size*/ + sizeof(T)/*count*/);
    }

    template<typename T> void* startArray(uint8_t code, const Descriptor* d, const Constructor& c)
    {
        void* token = start<T>(code, d);
        if (c.isDescribed) {
            writeDescriptor(c.descriptor);
        }
        check(1);
        writeCode(c.code);
        return token;
    }

};

}} // namespace qpid::amqp

#endif  /*!QPID_AMQP_ENCODER_H*/
