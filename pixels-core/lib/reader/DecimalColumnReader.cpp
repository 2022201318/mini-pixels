//
// Created by yuly on 05.04.23.
//

#include "reader/DecimalColumnReader.h"

/**
 * The column reader of decimals.
 * <p><b>Note: it only supports short decimals with max precision and scale 18.</b></p>
 * @author hank
 */
DecimalColumnReader::DecimalColumnReader(std::shared_ptr<TypeDescription> type) : ColumnReader(type) {

}

void DecimalColumnReader::close() {

}

void DecimalColumnReader::read(std::shared_ptr<ByteBuffer> input, pixels::proto::ColumnEncoding & encoding, int offset,
                               int size, int pixelStride, int vectorIndex, std::shared_ptr<ColumnVector> vector,
                               pixels::proto::ColumnChunkIndex & chunkIndex, std::shared_ptr<PixelsBitMask> filterMask) {
    std::shared_ptr<DecimalColumnVector> columnVector =
            std::static_pointer_cast<DecimalColumnVector>(vector);

    // ????? decimal(5,2)
    if (type->getPrecision() != columnVector->getPrecision() || type->getScale() != columnVector->getScale()) {
        // ????????? decimal(5,2)????????????(5,2)
        columnVector->setPrecision(5);
        columnVector->setScale(2);
    }

    // ??????????
    if(offset == 0) {
        // TODO: check null
        ColumnReader::elementIndex = 0;
        isNullOffset = chunkIndex.isnulloffset();
    }

    int pixelId = elementIndex / pixelStride;
    bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    switch (columnVector->physical_type_) {
    case PhysicalType::INT16:
        for (int i = 0; i < size; i++) {
            std::memcpy((uint8_t *)columnVector->vector +
                            (vectorIndex + i) * sizeof(int16_t),
                        input->getPointer() + input->getReadPos(),
                        sizeof(int16_t));
            input->setReadPos(input->getReadPos() + sizeof(int64_t));
        }
        break;
    case PhysicalType::INT32:
        for (int i = 0; i < size; i++) {
            std::memcpy((uint8_t *)columnVector->vector +
                            (vectorIndex + i) * sizeof(int32_t),
                        input->getPointer() + input->getReadPos(),
                        sizeof(int32_t));
            input->setReadPos(input->getReadPos() + sizeof(int64_t));
        }
        break;
    case PhysicalType::INT64:
    case PhysicalType::INT128:
        columnVector->vector =
            (long *)(input->getPointer() + input->getReadPos());
        input->setReadPos(input->getReadPos() + size * sizeof(long));
        break;
    default:
        throw std::runtime_error(
            "DecimalColumnReader: Unexpected Physical Type");
    }

    // ??????????? INT64 ?? decimal(5,2)?
    for (int i = 0; i < size; i++) {
        int64_t value = reinterpret_cast<int64_t*>(columnVector->vector)[i];
        // ????????? decimal(5,2) ??
        value /= 100;  // ??????? 100 ????????? 100 ??? decimal(5,2)
        reinterpret_cast<int64_t*>(columnVector->vector)[i] = value;
    }
}
