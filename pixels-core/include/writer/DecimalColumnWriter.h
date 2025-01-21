#ifndef DUCKDB_DECIMALCOLUMNWRITER_H
#define DUCKDB_DECIMALCOLUMNWRITER_H

#include "encoding/RunLenIntEncoder.h"
#include "ColumnWriter.h"
#include "utils/EncodingUtils.h"
#include "vector/DecimalColumnVector.h"  // ?? DecimalColumnVector

class DecimalColumnWriter : public ColumnWriter {
public:
    // ????
    DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption);

    // ???????
    int write(std::shared_ptr<ColumnVector> vector, int length) override;

    // ???????? null ?
    bool decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) override;

private:
    // ????????
    bool runlengthEncoding;

    // ???????
    std::unique_ptr<RunLenIntEncoder> encoder;

    // ???????
    std::vector<long> curPixelVector;

    // ?????????
    void writeCurPartDecimal(std::shared_ptr<DecimalColumnVector> columnVector, int curPartLength, int curPartOffset);

    // ?????
    void newPixel();
};

#endif // DUCKDB_DECIMALCOLUMNWRITER_H

