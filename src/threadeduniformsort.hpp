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

#ifndef SRC_CPP_THREADEDUNIFORMSORT_HPP_
#define SRC_CPP_THREADEDUNIFORMSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <future>

#include "./disk.hpp"
#include "./util.hpp"

using namespace std;
namespace ThreadedUniformSort {

    inline uint32_t const num_threads = 1;
    inline uint32_t const first_sort_bits = 20;

    inline static bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
    {
        for (uint32_t i = 0; i < entry_len; i++)
            if (memory[i] != 0)
                return false;
        return true;
    }

    vector<vector<uint8_t>> SortThreadA(
    	FileDisk *input_disk,
    	uint64_t const start_offset,
      uint64_t const num_entries,
      uint32_t const entry_len,
      uint32_t const bits_begin,
      vector<atomic<uint64_t>>& bucket_lens)
    {
    	uint32_t num_buckets = 1U << first_sort_bits;
    	vector<vector<uint8_t>> sort_buckets(num_buckets);
      vector<uint32_t> thread_bucket_lens(num_buckets);
    	for (auto& i : sort_buckets)
    	{
    		i.reserve(num_entries/num_buckets);
    	}
    	for (uint64_t i = 0; i < num_entries; i++)
    	{
    		uint8_t * latest_entry = input_disk->read_mapped + start_offset + entry_len*i;
        uint32_t bucket_num = Util::ExtractNum(latest_entry, entry_len, bits_begin, first_sort_bits);
        sort_buckets[bucket_num].insert(sort_buckets[bucket_num].end(), latest_entry, latest_entry+entry_len);
        thread_bucket_lens[bucket_num]+= entry_len;
    	}
      for (uint64_t i; i < num_buckets; i++)
      {
        bucket_lens[i].fetch_add(thread_bucket_lens[i]);
      }
    	return sort_buckets;
    }

    void SortThreadB(
    	uint32_t entry_len,
      vector<vector<vector<uint8_t>>> sort_buckets,
      uint64_t bucket_sets_offset,
      uint64_t bucket_sets_for_thread,
      uint8_t * output,
      vector<atomic<uint64_t>> bucket_set_lens,
      uint32_t start_bits_to_ignore)
    {
    	for (uint64_t bucket_set_i = bucket_sets_offset; bucket_set_i < bucket_sets_for_thread; bucket_set_i++)
    	{
        uint64_t bucket_length = 0;
        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * bucket_set_lens[bucket_set_i]) bucket_length++;
        
    		// Insertion sort
    		for (uint64_t t = 0; t < sort_buckets.size(); t++)
    		{
            auto bucket = sort_buckets[t][bucket_set_i];
            for (auto src_it = bucket.begin(); src_it < bucket.end(); src_it += entry_len)
            {
                uint8_t swap_space[entry_len];
                // First unique bits in the entry give the expected position of it in the sorted array.
                // We take 'bucket_length' bits starting with the first unique one.
                uint64_t pos = Util::ExtractNum(&(*src_it), entry_len, start_bits_to_ignore, bucket_length) * entry_len;
                // As long as position is occupied by a previous entry...
                while (!IsPositionEmpty(output + pos, entry_len))
                {
                    // ...store there the minimum between the two and continue to push the higher one.
                    if (Util::MemCmpBits(output + pos, &(*src_it), entry_len, start_bits_to_ignore) > 0)
                    {
                        memcpy(swap_space, output + pos, entry_len);
                        memcpy(output + pos, &(*src_it), entry_len);
                        memcpy(&(*src_it), swap_space, entry_len);
                    }
                    pos += entry_len;
                }
                // Push the entry in the first free spot.
                memcpy(output + pos, &(*src_it), entry_len);
            }
    		}
        output += bucket_set_lens[bucket_set_i];
    	}
    }

    void SortToMemory(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
    	time_t start_time = std::time(NULL);
    	input_disk.Open();

        // Start of parallel execution
        vector<future<vector<vector<uint8_t>>>> thread_a_futures;
        uint64_t entries_left = num_entries;
        uint64_t offset = 0;
        uint32_t num_bucket_sets = 1U << first_sort_bits;
        vector<atomic<uint64_t>> bucket_set_lens(num_bucket_sets);
        fill(bucket_set_lens.begin(), bucket_set_lens.end(), 0);
        for (uint32_t i = 0; i < num_threads; i++)
        {
           	uint64_t entries_for_thread = (num_entries / num_threads)+1;
            if (entries_for_thread > entries_left)
            {
            	entries_for_thread = entries_left;
            }
            thread_a_futures.push_back(async(SortThreadA, &input_disk, offset, entries_for_thread, entry_len, bits_begin, ref(bucket_set_lens)));
            entries_left -= entries_for_thread;
            offset += entries_for_thread*entry_len;
        }
        
        vector<vector<vector<uint8_t>>> sort_buckets;
        for (auto& t : thread_a_futures) {
        	t.wait();
            sort_buckets.push_back(t.get());
        }

        std::cout << "    Thread stage A took " << std::time(NULL) - start_time << "seconds." << std::endl;
        time_t prev_time = time(NULL);
        
        std::cout << "    Bucket 0 has " << bucket_set_lens[0]/entry_len << " entries." << endl;
        std::cout << "    Bucket 1111 has " << bucket_set_lens[1111]/entry_len << " entries." << endl; 

        // Build list of bucket offsets
        vector<uint64_t> bucket_offsets;
        uint64_t offset_ctr = 0;
        for (auto& len : bucket_set_lens) {
          bucket_offsets.push_back(offset_ctr);
          offset_ctr += len;
        }

        // end of sort A, time for pass B
        vector<thread> thread_b_futures;
        uint64_t bucket_sets_offset = 0;
        for (uint32_t i = 0; i < num_threads; i++) {
            uint64_t num_bucket_sets_for_thread = (num_bucket_sets/num_threads)+1;
            if (bucket_sets_offset + num_bucket_sets_for_thread > num_bucket_sets)
            {
              num_bucket_sets_for_thread = (num_bucket_sets - (bucket_sets_offset + num_bucket_sets_for_thread));
            }
            thread_b_futures.push_back(thread(SortThreadB, entry_len, sort_buckets, bucket_sets_offset, num_bucket_sets_for_thread, memory+bucket_offsets[bucket_sets_offset], &bucket_set_lens, bits_begin+first_sort_bits));
            bucket_sets_offset += num_bucket_sets_for_thread;
        }
        
        for (auto& t : thread_b_futures)
        {
          t.join();
        }
        std::cout << "    Thread stage B took " << std::time(NULL) - prev_time << "seconds." << std::endl;
        prev_time = time(NULL);
        
        for (uint32_t i = 0; i < num_entries-1; i++)
        {
          if (!lexicographical_compare(
            memory+i, memory + i + entry_len,
            memory+i+entry_len, memory+i+entry_len+entry_len))
            {
              std::cout << "Index " << i << " is unsorted!" << endl;
            }
        }

        std::cout << "    Threaded uniform sort took " << std::time(NULL) - start_time << "seconds." << std::endl;
        
        assert(bucket_sets_offset == num_entries*entry_len);
        
    }

}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
