#include "DateColumnVector.h"
#include <string>
#include <stdexcept>
#include <algorithm>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding) : ColumnVector(len, encoding) {
    if (encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32, len * sizeof(int32_t));
    } else {
        this->dates = nullptr;
    }
    memoryUsage += (long) sizeof(int32_t) * len;
}

void DateColumnVector::close() {
    if (!closed) {
        if (encoding && dates != nullptr) {
            free(dates);
        }
        dates = nullptr;
        ColumnVector::close();
    }
}

void DateColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        std::cout << dates[i] << std::endl;
    }
}

DateColumnVector::~DateColumnVector() {
    if (!closed) {
        DateColumnVector::close();
    }
}

void DateColumnVector::add(std::string &value) {
    // ????????????? "2023-12-31"
    // ???????? 1970-01-01 ?????
    // ?????????????????????????????????????

    int days = 0;
    try {
        // ???? "YYYY-MM-DD" ?????
        // ????????????????????????
        size_t pos1 = value.find('-');
        size_t pos2 = value.find('-', pos1 + 1);
        int year = std::stoi(value.substr(0, pos1));
        int month = std::stoi(value.substr(pos1 + 1, pos2 - pos1 - 1));
        int day = std::stoi(value.substr(pos2 + 1));

        // ????????
        days = (year - 1970) * 365 + (month - 1) * 30 + day;  // ????????????
    } catch (const std::exception &e) {
        throw std::invalid_argument("Invalid date format");
    }

    add(days);
}

void DateColumnVector::add(bool value) {
    add(value ? 1 : 0);  // ???????? 1 ? 0??????????????
}

void DateColumnVector::add(int64_t value) {
    // ?? int64_t ????????
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    dates[writeIndex++] = static_cast<int32_t>(value);  // ? int64_t ??? int32_t ??
    isNull[writeIndex - 1] = false;
}

void DateColumnVector::add(int value) {
    // ?? int ????????
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    dates[writeIndex++] = static_cast<int32_t>(value);  // ? int ??? int32_t ??
    isNull[writeIndex - 1] = false;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length < size) {
        int32_t *oldDates = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32, size * sizeof(int32_t));
        if (preserveData && dates != nullptr) {
            std::copy(oldDates, oldDates + length, dates);
        }
        delete[] oldDates;
        memoryUsage += (long) sizeof(int32_t) * (size - length);
        resize(size);
    }
}
