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

#include "writer/IntegerColumnWriter.h"
#include "utils/BitUtils.h"
std::ostream& operator<<(std::ostream& os, const std::_Bit_iterator& it) {
    // Implement how to print the bit iterator here
    return os;
}

IntegerColumnWriter::IntegerColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) :
ColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    isLong = type->getCategory() == TypeDescription::Category::LONG;
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

#include <fstream>
#include <iostream>
#include <string>

int IntegerColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    std::cout << "In IntegerColumnWriter" << std::endl;

    // ?????????
    auto columnVector = std::static_pointer_cast<LongColumnVector>(vector);
    columnVector->print(4);
    const std::string filePath = "/tmp/timestamps.txt";
    std::ifstream inFile(filePath);
    
    int64_t* values = nullptr;
    std::vector<long> fileValues;
    if (inFile) {
        // ???????????????????
        std::cout << "File found. Reading data from file." << std::endl;
        std::string line;

        while (std::getline(inFile, line)) {
            try {
                long value = std::stol(line); // ??????? long ??
                std::cout<<value<<std::endl;
                fileValues.push_back(value);
            } catch (const std::exception& e) {
                std::cerr << "Error parsing value: " << line << std::endl;
            }
        }

        values = fileValues.data(); // ????????
        for (size_t i = 0; i < fileValues.size(); ++i) {
            std::cout << "values[" << i << "]: " << values[i] << std::endl;
        }
        // ???????????? size???????
    } 
     else {
         // ?????????????????? vector ????
         std::cout << "File not found or empty. Using vector data." << std::endl;
         auto columnVector = std::static_pointer_cast<LongColumnVector>(vector);
         if (!columnVector) {
             throw std::invalid_argument("Invalid vector type");
         }

         if (columnVector->isLongVector()) {
             values = columnVector->longVector;
             std::cout << "Using long vector data." << std::endl;
         } else {
             values = reinterpret_cast<long*>(columnVector->intVector); // ??? intVector?????? long*
             std::cout << "Using int vector data (converted to long)." << std::endl;
         }
     }
            std::cout << "values[0]: " << values[0] << std::endl;


    int curPartLength;         // Size of the partition which belongs to current pixel
    int curPartOffset = 0;     // Starting offset of the partition which belongs to current pixel
    int nextPartLength = size; // Size of the partition which belongs to next pixel

    // Calculate partition sizes and eliminate branch prediction inside the loop
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartLong(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartLong(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}


void IntegerColumnWriter::close()
{
    if (runlengthEncoding && encoder)
    {
        encoder->clear();
    }
    ColumnWriter::close();
}
void IntegerColumnWriter::writeCurPartLong(std::shared_ptr<ColumnVector> columnVector, long *values, int curPartLength, int curPartOffset)
{
    std::cout << "Starting writeCurPartLong with curPartLength: " << curPartLength << ", curPartOffset: " << curPartOffset << std::endl;

    for (int i = 0; i < curPartLength; i++)
    {
        std::cout << "Processing index: " << i + curPartOffset << std::endl;
        curPixelEleIndex++;
        
        if (columnVector->isNull[i + curPartOffset])
        {
            std::cout << "duandian1 " << std::endl;
            hasNull = true;
            if (nullsPadding)
            {
                std::cout << "duandian2 " << std::endl;
                std::cout << "Padding 0 for null at index: " << i + curPartOffset << std::endl;
                // padding 0 for nulls
                curPixelVector[curPixelVectorIndex++] = 0L;
            }
        }
        else
        {
            std::cout << "duandian3 " << std::endl;
            std::cout << "Assigning value: " << values[i + curPartOffset] << " to curPixelVector at index: " << curPixelVectorIndex << std::endl;
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }

    std::cout << "Copying isNull array from " << (columnVector->isNull + curPartOffset) << " to " << (isNull.begin() + curPixelIsNullIndex) << std::endl;
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    
    curPixelIsNullIndex += curPartLength;

    std::cout << "Finished writeCurPartLong, current curPixelIsNullIndex: " << curPixelIsNullIndex << ", curPixelVectorIndex: " << curPixelVectorIndex << std::endl;
}


bool IntegerColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption->isNullsPadding();
}

void IntegerColumnWriter::newPixel()
{
    // write out current pixel vector
    if (runlengthEncoding)
    {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(int));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    }
    else
    {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        if (isLong)
        {
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
        }
        else
        {
            curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(int));
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
            {
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    encodingUtils.writeIntLE(curVecPartitionBuffer, (int)curPixelVector[i]);
                }
            }
            else
            {
                for (int i = 0; i < curPixelVectorIndex; i++)
                {
                    encodingUtils.writeIntBE(curVecPartitionBuffer, (int)curPixelVector[i]);
                }
            }
        }
        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }

    ColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding IntegerColumnWriter::getColumnChunkEncoding()
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