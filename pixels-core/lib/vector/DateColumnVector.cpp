#include "vector/DateColumnVector.h"
#include <algorithm>
#include <iostream>  // ??????
#include <iostream>
#include <string>
#include <stdexcept>
#include <ctime>
#include <sstream>
DateColumnVector::DateColumnVector(uint64_t len, bool encoding)
    : ColumnVector(len, encoding) {
    // ???????????????
    std::cout << "Initializing DateColumnVector with size: " << len << std::endl;

    // ???????????
    posix_memalign(reinterpret_cast<void **>(&dates), 32, len * sizeof(int32_t));
    memoryUsage += sizeof(int) * len;  // ??????

    // ????????????
    std::cout << "Memory usage for dates array: " << memoryUsage << " bytes" << std::endl;
}

DateColumnVector::~DateColumnVector() {
    if (!closed) {
        DateColumnVector::close();
    }
}

void DateColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (encoding && dates != nullptr) {
            free(dates);
            std::cout << "Memory for dates array freed" << std::endl;
        }
        dates = nullptr;
    }
}

void * DateColumnVector::current() {
    if (dates == nullptr) {
        std::cout << "Dates array is null" << std::endl;  // ????
        return nullptr;
    } else {
        std::cout << "Returning current date value: " << dates[readIndex] << std::endl;  // ????
        return dates + readIndex;
    }
}

void DateColumnVector::add(std::string &value) {
    std::cout << "Adding value: " << value << std::endl;
    try {
        // ???? "true" ? "false"????? 1 ? 0
        if (value == "true") {
            add(1);
        } else if (value == "false") {
            add(0);
        } else {
            // ????????????
            int year, month, day;
            char dash;
            std::istringstream ss(value);
            ss >> year >> dash >> month >> dash >> day;

            if (ss.fail()) {
                throw std::invalid_argument("Invalid date format");
            }

            // ???????? 1970-01-01 ??
            struct tm tm = {};
            tm.tm_year = year - 1900;  // year since 1900
            tm.tm_mon = month - 1;     // month is 0-based
            tm.tm_mday = day;

            std::time_t time_since_epoch = std::mktime(&tm);
            if (time_since_epoch == -1) {
                throw std::runtime_error("Failed to convert date to time_t");
            }

            int days = static_cast<int>(time_since_epoch / (60 * 60 * 24));  // ?????
            add(days);  // ??????????
        }
    } catch (const std::exception &e) {
        std::cerr << "Error processing date string: " << e.what() << std::endl;
    }
}



void DateColumnVector::add(int value) {
    std::cout << "Adding int value: " << value << std::endl;  // ????
    if (writeIndex >= 2*length) {
        ensureSize(writeIndex * 4, true);  // ????
    }

    int index = writeIndex++;
    dates[index] = value;  // ?????
    isNull[index] = false;  // ??????
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    std::cout << "Ensuring size, requested size: " << size << std::endl;  // ????
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        int *oldDates = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32, size * sizeof(int32_t));
        if (preserveData) {
            std::copy(oldDates, oldDates + length, dates);  // ?????
        }
        delete[] oldDates;  // ?????
        memoryUsage += sizeof(int) * (size - length);  // ??????
        resize(size);  // ????
    }
}

void DateColumnVector::set(int elementNum, int days) {
    std::cout << "Setting date for element " << elementNum << " to: " << days << std::endl;  // ????
    if (elementNum < 0 || elementNum >= length) {
        throw std::out_of_range("Index out of bounds in set method.");
    }
    dates[elementNum] = days;
}

bool DateColumnVector::isDateVector() {
    return true;  // ??????
}

void DateColumnVector::print(int rowCount) {
    std::cout << "DateColumnVector data: ";
    for (int i = 0; i < rowCount; ++i) {
        std::cout << dates[i] << (i < rowCount - 1 ? ", " : "\n");
    }
}
