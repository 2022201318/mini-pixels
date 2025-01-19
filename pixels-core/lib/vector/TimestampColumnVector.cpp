#include "vector/TimestampColumnVector.h"
#include <stdexcept>
#include <iostream>
#include <string>
#include <algorithm>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding)
    : ColumnVector(len, encoding) {
    this->precision = precision;
    if (encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64, len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}

void TimestampColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        std::cout << times[i] << std::endl;
    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if (!closed) {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current() {
    if (this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
 * ?????????
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::add(std::string &value) {
    // ????????????????????"2023-12-31T12:34:56"?
    // ????????????????????????????????????
    long timestamp = 0;

    try {
        // ????????????????
        // ?????????????????????? long ??????
        timestamp = std::stol(value);  // ???????????
    } catch (const std::exception &e) {
        throw std::invalid_argument("Invalid timestamp format");
    }

    add(timestamp);
}

void TimestampColumnVector::add(bool value) {
    // ???????????????
    // ???true ???????????false ???????
    long timestamp = value ? 1 : 0;  // ?? 1 ? 0 ??????????
    add(timestamp);
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    times[writeIndex++] = value;  // ? int64_t ??? long ???
    isNull[writeIndex - 1] = false;
}

void TimestampColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    times[writeIndex++] = static_cast<long>(value);  // ? int ??? long ???
    isNull[writeIndex - 1] = false;
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length < size) {
        long *oldTimes = times;
        posix_memalign(reinterpret_cast<void **>(&times), 64, size * sizeof(long));
        if (preserveData && times != nullptr) {
            std::copy(oldTimes, oldTimes + length, times);
        }
        delete[] oldTimes;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}
