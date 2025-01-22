#include "vector/TimestampColumnVector.h"
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <string>
#include <algorithm>
#include <fstream>
TimestampColumnVector::TimestampColumnVector(int precision, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding), precision(precision) {
    std::cout << "[DEBUG] Constructor called with default size.\n";
    if (encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64, VectorizedRowBatch::DEFAULT_SIZE * sizeof(long));
    } else {
        this->times = nullptr;
    }
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding)
    : ColumnVector(len, encoding), precision(precision) {
    std::cout << "[DEBUG] Constructor called with size: " << len << "\n";
    
    posix_memalign(reinterpret_cast<void **>(&this->times), 64, len * sizeof(long));
}

void TimestampColumnVector::close() {
    if (!closed) {
        std::cout << "[DEBUG] Closing column vector.\n";
        ColumnVector::close();
        if (encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {

    std::string filePath = "/tmp/timestamps.txt";

    // ?????ofstream ??????????????
    std::ofstream outFile(filePath);

    std::cout << "[DEBUG] Printing " << rowCount << " timestamps to file.\n";
    for (int i = 0; i < rowCount; i++) {
        outFile << times[i] << std::endl;  // ??????????
    }

    outFile.close();  // ????
    std::cout << "[DEBUG] Timestamps written to file successfully.\n";
}
TimestampColumnVector::~TimestampColumnVector() {
    std::cout << "[DEBUG] Destructor called.\n";
    if (!closed) {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current() {
    return this->times;
}

void TimestampColumnVector::set(int elementNum, long ts) {
    std::cout << "[DEBUG] Setting timestamp at index " << elementNum << " to value " << ts << "\n";
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
}

void TimestampColumnVector::add(std::string &value) {
    std::cout << "[DEBUG] Adding timestamp from string: " << value << "\n";

    // Expected format: "YYYY-MM-DDTHH:MM:SS"
    std::cout <<value.length()<<" "<<value[4]<<" "<<value[7]<<std::endl;
    if (value.length() != 19 || value[4] != '-' || value[7] != '-' || value[10] != ' ' || 
        value[13] != ':' || value[16] != ':') {
        throw std::invalid_argument("Invalid timestamp format: " + value);
    }

    try {
        int year = std::stoi(value.substr(0, 4));
        int month = std::stoi(value.substr(5, 2));
        int day = std::stoi(value.substr(8, 2));
        int hour = std::stoi(value.substr(11, 2));
        int minute = std::stoi(value.substr(14, 2));
        int second = std::stoi(value.substr(17, 2));

        std::tm tm = {};
        tm.tm_year = year - 1900; // Year since 1900
        tm.tm_mon = month - 1;   // Months are 0-based
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min = minute;
        tm.tm_sec = second;

        // Convert to time_t (seconds since epoch) and then to microseconds
        std::time_t t = std::mktime(&tm);
        if (t == -1) {
            throw std::invalid_argument("Failed to convert timestamp: " + value);
        }

        long timestamp = (static_cast<long>(t)+8*3600) * 1000000;
        add(timestamp);
    } catch (const std::exception &e) {
        throw std::invalid_argument("Error parsing timestamp: " + value + ". " + e.what());
    }
}


void TimestampColumnVector::add(bool value) {
    std::cout << "[DEBUG] Adding boolean value: " << value << "\n";
    long timestamp = value ? 1 : 0;
    add(timestamp);
}

void TimestampColumnVector::add(int64_t value) {
    std::cout << "[DEBUG] Adding int64_t value: " << value << "\n";
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    times[writeIndex++] = value;
    isNull[writeIndex - 1] = false;
}

void TimestampColumnVector::add(int value) {
    std::cout << "[DEBUG] Adding int value: " << value << "\n";
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    times[writeIndex++] = static_cast<long>(value);
    isNull[writeIndex - 1] = false;
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    std::cout << "[DEBUG] Ensuring size: " << size << " (current length: " << length << ")\n";
    if (length < size) {
        long *oldTimes = times;
        posix_memalign(reinterpret_cast<void **>(&times), 64, size * sizeof(long));
        if (preserveData && oldTimes != nullptr) {
            std::copy(oldTimes, oldTimes + length, times);
        }
        delete[] oldTimes;
        memoryUsage += static_cast<long>(sizeof(long)) * (size - length);
        resize(size);
    }
}
