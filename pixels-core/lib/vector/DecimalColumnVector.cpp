#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"
#include <cmath>
#include <iostream>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(10,2), is 314. The precision and scale
 * of this decimal are fixed at 10 and 2, respectively.
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, 10, 2, encoding);  // ??? decimal(10,2)
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding)
    : ColumnVector(len, encoding) {
    this->precision = 5;  // ?????10
    this->scale = 2;       // ??????2

    physical_type_ = PhysicalType::INT64;
    posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int64_t));
    memoryUsage += (uint64_t)sizeof(int64_t) * len;
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        free(vector);
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        int64_t value = reinterpret_cast<int64_t*>(vector)[i];
        double decimalValue = static_cast<double>(value) / 100.0;  // ?????
        std::cout << "Row " << i << ": " << decimalValue << std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if (!closed) {
        DecimalColumnVector::close();
    }
}

void *DecimalColumnVector::current() {
    if (vector == nullptr) {
        return nullptr;
    } else {
        return reinterpret_cast<int64_t*>(vector) + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
    return precision;
}

int DecimalColumnVector::getScale() {
    return scale;
}

// ?? decimal(10,2) ??????
void DecimalColumnVector::add(std::string &value) {
    int64_t decimalValue = 0;
    try {
        double parsedValue = std::stod(value);
        decimalValue = static_cast<int64_t>(parsedValue*100);  // ?? 10^2 ??????
    } catch (const std::exception &e) {
        throw std::invalid_argument("Invalid decimal string format: " + value);
    }
    add(decimalValue);
}

// ?????
void DecimalColumnVector::add(bool value) {
    int64_t decimalValue = value ? 100 : 0;  // 1.00 or 0.00
    add(decimalValue);
}
void DecimalColumnVector::setPrecision(int precision) {
        this->precision = precision;
}

void DecimalColumnVector::setScale(int scale) {
        this->scale = scale;
}
// ?? int64 ?
void DecimalColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    reinterpret_cast<int64_t*>(vector)[writeIndex++] = value;
    isNull[writeIndex - 1] = false;
}

// ?? int ?
void DecimalColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    value *= 100;  // ?? scale = 2
    reinterpret_cast<int64_t*>(vector)[writeIndex++] = value;
    isNull[writeIndex - 1] = false;
}

// ????????
void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length < size) {
        int64_t *oldVector = reinterpret_cast<int64_t*>(vector);
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(int64_t));
        if (preserveData && oldVector != nullptr) {
            std::copy(oldVector, oldVector + length, reinterpret_cast<int64_t*>(vector));
        }
        free(oldVector);
        memoryUsage += (uint64_t)sizeof(int64_t) * (size - length);
        resize(size);
    }
}
