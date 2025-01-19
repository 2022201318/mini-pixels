#ifndef DUCKDB_DATECOLUMNVECTOR_H
#define DUCKDB_DATECOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class DateColumnVector : public ColumnVector {
public:
    /*
     * ?????1970?1?1??????????????Presto?????????
     */
    int *dates;

    /**
     * ??????????????????????
     */
    explicit DateColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);

    ~DateColumnVector();

    // ????????
    void *current() override;
    void print(int rowCount) override;
    void close() override;

    // ??????????
    void add(std::string &value) override;
    void add(bool value) override;
    void add(int64_t value) override;
    void add(int value) override;

    // ????????
    void set(int elementNum, int days);

    // ????????????
    void ensureSize(uint64_t size, bool preserveData) override;
};

#endif // DUCKDB_DATECOLUMNVECTOR_H

