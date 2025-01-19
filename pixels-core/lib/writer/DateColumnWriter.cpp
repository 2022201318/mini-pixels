/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include "writer/DateColumnWriter.h"
#include "utils/BitUtils.h"

DateColumnWriter::DateColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) :
    ColumnWriter(type, writerOption), curPixelVector(pixelStride) 
{
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);  // Check if runlength encoding is enabled
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

int DateColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size) 
{
    std::cout << "In DateColumnWriter" << std::endl;
    auto columnVector = std::static_pointer_cast<LongColumnVector>(vector);
    if (!columnVector) 
    {
        throw std::invalid_argument("Invalid vector type");
    }

    long* longValues = columnVector->longVector;
    int* intValues = reinterpret_cast<int*>(longValues);  // ? long* ??? int*???????????

    int curPartLength;          // ???????????
    int curPartOffset = 0;      // ???????????
    int nextPartLength = size;  // ?????????

    // ?????????? for ???????????
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) 
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTime(columnVector, intValues, curPartLength, curPartOffset);  // ?? int* ? writeCurPartTime
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartTime(columnVector, intValues, curPartLength, curPartOffset);  // ?? int* ? writeCurPartTime

    return outputStream->getWritePos();
}



void DateColumnWriter::close() 
{
    if (runlengthEncoding && encoder) 
    {
        encoder->clear();
    }
    ColumnWriter::close();
}

void DateColumnWriter::writeCurPartTime(std::shared_ptr<ColumnVector> columnVector, int* values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++) 
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) 
        {
            hasNull = true;
            if (nullsPadding) 
            {
                // ???????
                curPixelVector[curPixelVectorIndex++] = 0L;
            }
        } 
        else 
        {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }

    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

bool DateColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) 
{
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2)) 
    {
        return false;
    }
    return writerOption->isNullsPadding();
}

void DateColumnWriter::newPixel() 
{
    // ??????????
    if (runlengthEncoding) 
    {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(long));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    } 
    else 
    {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(long));

        if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) 
        {
            for (int i = 0; i < curPixelVectorIndex; i++) 
            {
                encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
            }
        } 
        else 
        {
            for (int i = 0; i < curPixelVectorIndex; i++) 
            {
                encodingUtils.writeLongBE(curVecPartitionBuffer, curPixelVector[i]);
            }
        }

        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }

    ColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding DateColumnWriter::getColumnChunkEncoding() const
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding) 
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    } 
    else 
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}
