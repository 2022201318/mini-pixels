#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"
#include <cmath>  // Ensure this header is included

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding)
    : ColumnVector(len, encoding) {
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(int64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(__int128));  // Use __int128 here
        memoryUsage += (uint64_t)sizeof(__int128) * len;  // Use __int128 here
    } else {
        throw std::runtime_error("Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        std::cout << vector[i] << std::endl;
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
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
    return precision;
}

int DecimalColumnVector::getScale() {
    return scale;
}

void DecimalColumnVector::add(std::string &value) {
    long decimalValue = 0;
    try {
        decimalValue = std::stol(value);  // Directly convert string to integer
        decimalValue *= static_cast<long>(std::pow(10, scale));  // Multiply by 10^scale
    } catch (const std::exception &e) {
        throw std::invalid_argument("Invalid decimal string format");
    }
    add(decimalValue);
}

void DecimalColumnVector::add(bool value) {
    long decimalValue = value ? 1 : 0;  // Use 1 or 0 for decimals
    add(decimalValue);
}

void DecimalColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    value *= static_cast<int64_t>(std::pow(10, scale));  // Adjust value by 10^scale
    vector[writeIndex++] = value;
    isNull[writeIndex - 1] = false;
}

void DecimalColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    value *= static_cast<int>(std::pow(10, scale));  // Adjust value by 10^scale
    vector[writeIndex++] = value;
    isNull[writeIndex - 1] = false;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length < size) {
        long *oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(long));
        if (preserveData && oldVector != nullptr) {
            std::copy(oldVector, oldVector + length, vector);
        }
        delete[] oldVector;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}

