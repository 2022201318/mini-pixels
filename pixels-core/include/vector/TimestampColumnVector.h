#ifndef DUCKDB_TIMESTAMPCOLUMNVECTOR_H
#define DUCKDB_TIMESTAMPCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <string>
#include <algorithm>
class TimestampColumnVector : public ColumnVector {
public:
    int precision;
    int64_t *times; // ??????long?

    /**
     * ?????????????????????
     */
    explicit TimestampColumnVector(int precision, bool encoding = false);

    explicit TimestampColumnVector(uint64_t len, int precision, bool encoding = false);

    // ???????
    void *current() override;
    void set(int elementNum, long ts);
    void print(int rowCount) override;
    void close() override;
    void *current1();
    // ????????
    void add(std::string &value) override;
    void add(bool value) override;
    void add(int64_t value) override;
    void add(int value) override;

    // ????????????
    void ensureSize(uint64_t size, bool preserveData) override;

    ~TimestampColumnVector();
};

#endif // DUCKDB_TIMESTAMPCOLUMNVECTOR_H

