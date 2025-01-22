#ifndef PIXELS_DECIMALCOLUMNVECTOR_H
#define PIXELS_DECIMALCOLUMNVECTOR_H

#include "duckdb/common/types.hpp"
#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

using PhysicalType = duckdb::PhysicalType;

class DecimalColumnVector : public ColumnVector {
public:
    long *vector;  // ?? Decimal ???????
    int precision;  // ??
    int scale;  // ????
    PhysicalType physical_type_;  // ????
    static long DEFAULT_UNSCALED_VALUE;  // ??????????

    /**
     * ????????????????????
     */
    DecimalColumnVector(int precision, int scale, bool encoding = false);
    DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding = false);
    ~DecimalColumnVector();
    
    // ??????
    void print(int rowCount) override;
    void close() override;
    void *current() override;
    
    // ?????????
    int getPrecision();
    int getScale();
    void setPrecision(int precision);

    void setScale(int scale);

    void add(std::string &value) override;
    void add(bool value) override;
    void add(int64_t value) override;
    void add(int value) override;

    // ????????????
    void ensureSize(uint64_t size, bool preserveData) override;
};

#endif // PIXELS_DECIMALCOLUMNVECTOR_H
