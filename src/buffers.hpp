// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_CPP_BUFFERS_HPP_
#define SRC_CPP_BUFFERS_HPP_


#include <sys/mman.h>
#include <sys/stat.h>

struct Buffer 
{
    uint8_t *data = NULL;
    uint64_t data_len = 0;
    std::atomic<uint64_t>* insert_pos;

    explicit Buffer(const uint64_t size)
    {
        data_len = size;
        insert_pos = new std::atomic<uint64_t>(0);
        data = (uint8_t *) mmap(NULL, size, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        if (data == MAP_FAILED)
        {
            perror("Error mmapping!");
            exit(EXIT_FAILURE);
        }
    }
    
    
    uint64_t GetInsertionOffset(uint64_t len)
    {
      return insert_pos->fetch_add(len);
    }

    ~Buffer()
    {
      munmap(mapped_data, file_len);
    }
}


#endif  // SRC_CPP_BUFFERS_HPP_
