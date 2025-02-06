//
// Created by Borelset on 2020/3/24.
//

#ifndef ODESSSTORAGE_CHUNKFILEMANAGER_H
#define ODESSSTORAGE_CHUNKFILEMANAGER_H

#include <unordered_map>
#include "FileOperator.h"

extern std::string ChunkFilePath;

class ChunkFileManager{
public:

    static FileOperator* get(uint64_t i){
        char buffer[256];
        sprintf(buffer, FLAGS_ChunkFilePath.data(), i);
        FileOperator* temp = new FileOperator(buffer, FileOpenType::Read);
        return temp;
    }

    static std::string path;

};


#endif //ODESSSTORAGE_CHUNKFILEMANAGER_H
