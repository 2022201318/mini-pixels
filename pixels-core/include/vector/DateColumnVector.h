#ifndef DUCKDB_DATECOLUMNVECTOR_H
#define DUCKDB_DATECOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class DateColumnVector : public ColumnVector {
public:
    int *dates;

    /**
     * ????????????
     */
    explicit DateColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);

    ~DateColumnVector();

    // ??????????
    void *current() override;
    // ??????????
    void close() override;
    void set(int elementNum, int days);

    // ?????????
    void add(std::string &value) override;
    void add(int value) override;
    void print(int rowCount) override;
    // ???????????
    void ensureSize(uint64_t size, bool preserveData) override;

    // ???????????
    bool isDateVector();  // ??????
};

#endif // DUCKDB_DATECOLUMNVECTOR_H

