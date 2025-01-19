#include "writer/DecimalColumnWriter.h"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption) {
    // ???????
    runlengthEncoding = writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>();
    }

    // ???????????
    curPixelVector = std::vector<long>(pixelStride, 0);
}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size) {
    std::cout << "In DecimalColumnWriter" << std::endl;

    // ????? DecimalColumnVector ??
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    if (!columnVector) {
        throw std::invalid_argument("Invalid vector type");
    }

    long* values = columnVector->vector;  // ??????????
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartDecimal(columnVector, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartDecimal(columnVector, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

void DecimalColumnWriter::writeCurPartDecimal(std::shared_ptr<DecimalColumnVector> columnVector, int curPartLength, int curPartOffset) {
    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) {
            hasNull = true;
            if (nullsPadding) {
                curPixelVector[curPixelVectorIndex++] = 0L;  // ?? 0 ?? null ?
            }
        } else {
            curPixelVector[curPixelVectorIndex++] = columnVector->vector[i + curPartOffset];  // ?? vector ????
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, 
              isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void DecimalColumnWriter::newPixel() {
    if (runlengthEncoding) {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(long));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    } else {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(long));

        for (int i = 0; i < curPixelVectorIndex; i++) {
            encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
        }

        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }

    ColumnWriter::newPixel();
}

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2)) {
        return false;  // ??? null ??
    }
    return writerOption->isNullsPadding();  // ?????????? null ??
}
