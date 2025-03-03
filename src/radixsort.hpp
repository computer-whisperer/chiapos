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

#ifndef SRC_CPP_RADIXSORT_HPP_
#define SRC_CPP_RADIXSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <future>

#include "./buffers.hpp"
#include "./util.hpp"

using namespace std;
namespace RadixSort {

    inline uint64_t const first_sort_bits = 12;
    inline uint64_t const bits_per_radix_stage = 14;

    inline static bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
    {
        for (uint32_t i = 0; i < entry_len; i++)
            if (memory[i] != 0)
                return false;
        return true;
    }

    vector<vector<uint8_t>>* SortThreadA(
    	Buffer *input_buff,
    	uint64_t const start_offset,
      uint64_t const num_entries,
      uint32_t const entry_len,
      uint32_t const bits_begin,
      vector<atomic<uint64_t>*>* bucket_lens)
    {
    	uint32_t num_buckets = 1U << first_sort_bits;
    	vector<vector<uint8_t>>* sort_buckets = new vector<vector<uint8_t>>(num_buckets);
      vector<uint32_t> thread_bucket_lens(num_buckets);
      fill(thread_bucket_lens.begin(), thread_bucket_lens.end(), 0);
    	for (auto& i : *sort_buckets)
    	{
    		i.reserve(entry_len*num_entries*1.5/num_buckets);
    	}
    	for (uint64_t i = 0; i < num_entries; i++)
    	{
    		uint8_t latest_entry[100];
        memcpy(latest_entry, input_buff->data + start_offset + entry_len*i, entry_len);
        uint64_t bucket_num = Util::ExtractNum(latest_entry, entry_len, bits_begin, first_sort_bits);
        (*sort_buckets)[bucket_num].insert((*sort_buckets)[bucket_num].end(), latest_entry, latest_entry+entry_len);
        thread_bucket_lens[bucket_num] += entry_len;
    	}
      for (uint64_t i = 0; i < num_buckets; i++)
      {
        (*bucket_lens)[i]->fetch_add(thread_bucket_lens[i]);
      }
    	return sort_buckets;
    }

    void SortThreadB(
    	uint32_t entry_len,
      vector<vector<vector<uint8_t>>*>* sort_buckets,
      uint64_t bucket_sets_offset,
      uint64_t bucket_sets_for_thread,
      uint8_t * output,
      vector<uint64_t>* bucket_set_lens,
      uint32_t const start_bits_to_ignore)
    {
    	for (uint64_t bucket_set_i = bucket_sets_offset; bucket_set_i < bucket_sets_offset+bucket_sets_for_thread; bucket_set_i++)
    	{
        if ((*bucket_set_lens)[bucket_set_i] == 0)
        {
          continue;
        }
        // Index all inputs onto the same array
        vector<uint8_t*>* entry_ptrs = new vector<uint8_t*>((*bucket_set_lens)[bucket_set_i]/entry_len);
        vector<uint8_t*>* entry_ptrs_temp = new vector<uint8_t*>((*bucket_set_lens)[bucket_set_i]/entry_len);
        {
          uint64_t entry_ptrs_i = 0;
          for (uint64_t t = 0; t < sort_buckets->size(); t++)
          {
            if ((*(*sort_buckets)[t])[bucket_set_i].size() == 0)
            {
              continue;
            }
            for (auto src_it = (*(*sort_buckets)[t])[bucket_set_i].begin(); src_it < (*(*sort_buckets)[t])[bucket_set_i].end(); src_it += entry_len)
            {
              assert(&(*src_it) != NULL);
              (*entry_ptrs)[entry_ptrs_i] = &(*src_it);
              entry_ptrs_i++;
            }
          }
          assert(entry_ptrs_i*entry_len == (*bucket_set_lens)[bucket_set_i]);
        }
    		int32_t checking_bit_start = (entry_len*8) - bits_per_radix_stage;
        int32_t checking_bit_end = checking_bit_start + bits_per_radix_stage;
        // Radix sort
        while (checking_bit_start < checking_bit_end)
        {
          uint32_t bits_for_stage = checking_bit_end - checking_bit_start;
          // Counting sort on bitrange of data
          vector<uint32_t> counts(1ULL << bits_for_stage);
          fill(counts.begin(), counts.end(), 0);
          for (auto & entry_ptr : *entry_ptrs)
          {
        	uint8_t buff[entry_len];
        	memcpy(buff, entry_ptr, entry_len);
            uint64_t num = Util::SliceInt64FromBytes(buff, checking_bit_start, bits_for_stage);
            counts[num]++;
          }
          // Running add to counts array
          vector<uint32_t> partial_sums((1ULL << bits_for_stage)+1);
          partial_sums[0] = 0;
          partial_sum(counts.begin(), counts.end(), partial_sums.begin()+1);
         // uint64_t running_total = 0;
         // for (auto & count : counts)
         // {
         //   uint64_t temp = running_total;
         //   running_total += count;
         //   count = temp;
         //   
         // }
          // Rebuild pointer map
          for (auto & entry_ptr : *entry_ptrs)
          {
          	uint8_t buff[entry_len];
          	memcpy(buff, entry_ptr, entry_len);
            uint64_t num = Util::SliceInt64FromBytes(buff, checking_bit_start, bits_for_stage);
            (*entry_ptrs_temp)[partial_sums[num]] = entry_ptr;
            partial_sums[num]++;
          }
          // Swap vectors of entry pointers
          vector<uint8_t*>* tmp = entry_ptrs;
          entry_ptrs = entry_ptrs_temp;
          entry_ptrs_temp = tmp; 
          
          // confirm that the selected bits have been sorted

          uint64_t prev_val = 0;
          for (auto & entry_ptr : *entry_ptrs)
          {
          	uint8_t buff[entry_len];
          	memcpy(buff, entry_ptr, entry_len);
            uint64_t new_val = Util::SliceInt64FromBytes(buff, checking_bit_start, bits_for_stage);
            assert(prev_val <= new_val);
            prev_val = new_val;
          }
          
          // Advance to next bit set for sorting
          checking_bit_end = checking_bit_start;
          checking_bit_start -= bits_per_radix_stage;
          if (checking_bit_start < (int32_t)start_bits_to_ignore)
          {
            checking_bit_start = (int32_t)start_bits_to_ignore;
          }
          assert(checking_bit_start >= 0);
        }
        
        // confirm that everything has been sorted      
/*
        for (uint64_t i = 1; i < (*bucket_set_lens)[bucket_set_i]/entry_len; i++)
        {
          assert (lexicographical_compare(
            (*entry_ptrs)[i-1], (*entry_ptrs)[i-1] + entry_len,
            (*entry_ptrs)[i], (*entry_ptrs)[i]+entry_len));
        }
  */

        // entry_ptrs should now be sorted, copy to dest
        for (auto & entry_ptr : *entry_ptrs)
        {
          memcpy(output, entry_ptr, entry_len);
          output += entry_len;
        }

        delete entry_ptrs;
        delete entry_ptrs_temp;
    	}
    }

    unsigned long hash_with_len(unsigned char *str, size_t len)
    {
        unsigned long hash = 5381;
        int c;
        int i;

        for (i = 0; i < len; i++)
        {
        	c = *str++;
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        }

        return hash;
    }

    Buffer* SortToMemory(
        Buffer * input_buff,
        uint32_t const bits_begin,
        uint32_t const num_threads)
    {
    	time_t start_time = std::time(NULL);
    	uint64_t num_entries = input_buff->Count();
    	uint32_t entry_len = input_buff->entry_len;

    	assert(num_entries*entry_len <= input_buff->data_len);

      
      // Get checksum of input data first
      uint64_t csum = 0;
      for (uint64_t i = 0; i < num_entries; i++)
      {
        csum += hash_with_len(input_buff->data + i*input_buff->entry_len, input_buff->entry_len);
      }


        // Start of parallel execution
        vector<future<vector<vector<uint8_t>>*>> thread_a_futures;
        uint64_t entries_left = num_entries;
        uint64_t offset = 0;
        uint32_t num_bucket_sets = 1U << first_sort_bits;
        vector<atomic<uint64_t>*> bucket_set_lens;
        for (uint32_t i = 0; i < num_bucket_sets; i++)
        {
          bucket_set_lens.push_back(new atomic<uint64_t>(0));
        }
        for (uint32_t i = 0; i < num_threads; i++)
        {
           	uint64_t entries_for_thread = (num_entries / num_threads)+1;
            if (entries_for_thread > entries_left)
            {
            	entries_for_thread = entries_left;
            }
            thread_a_futures.push_back(async(SortThreadA, input_buff, offset, entries_for_thread, entry_len, bits_begin, &bucket_set_lens));
            entries_left -= entries_for_thread;
            offset += entries_for_thread*entry_len;
        }
        
        vector<vector<vector<uint8_t>>*> sort_buckets;
        for (auto& t : thread_a_futures) {
        	t.wait();
            sort_buckets.push_back(t.get());
        }

        delete input_buff;
        Buffer * output_buff = new Buffer(num_entries*entry_len);
        output_buff->entry_len = entry_len;
        *output_buff->insert_pos = num_entries*entry_len;

        std::cout << "    Thread stage A took " << std::time(NULL) - start_time << "seconds." << std::endl;
        time_t prev_time = time(NULL);
        
        std::cout << "    Bucket 0 has " << *(bucket_set_lens)[0]/entry_len << " entries." << endl;

        // Build list of bucket offsets
        vector<uint64_t> bucket_offsets;
        vector<uint64_t> bucket_set_lens_notatomic;
        uint64_t offset_ctr = 0;
        for (auto& len : bucket_set_lens) {
          bucket_offsets.push_back(offset_ctr);
          bucket_set_lens_notatomic.push_back(*len);
          offset_ctr += *len;
        }

        // end of sort A, time for pass B
        vector<thread> thread_b_futures;
        uint64_t bucket_sets_offset = 0;
        for (uint32_t i = 0; i < num_threads; i++) {
            uint64_t num_bucket_sets_for_thread = (num_bucket_sets/num_threads)+1;
            if ((bucket_sets_offset + num_bucket_sets_for_thread) > num_bucket_sets)
            {
              num_bucket_sets_for_thread = num_bucket_sets - bucket_sets_offset;
            }
            
            thread_b_futures.push_back(thread(SortThreadB, entry_len, &sort_buckets, bucket_sets_offset, num_bucket_sets_for_thread, output_buff->data+bucket_offsets[bucket_sets_offset], &bucket_set_lens_notatomic, bits_begin+first_sort_bits));
            bucket_sets_offset += num_bucket_sets_for_thread;
        }
        
        for (auto& t : thread_b_futures)
        {
          t.join();
        }
        std::cout << "    Thread stage B took " << std::time(NULL) - prev_time << "seconds." << std::endl;
        prev_time = time(NULL);
     
/*
        for (uint32_t i = 0; i < num_entries-1; i++)
        {
          if (!lexicographical_compare(
            output_buff->data+i*entry_len, output_buff->data + i*entry_len + entry_len,
            output_buff->data+i*entry_len+entry_len, output_buff->data+i*entry_len+entry_len+entry_len))
            {
              std::cout << "Index " << i << " is unsorted!" << endl;
            }
        }
*/
        std::cout << "    Threaded radix sort took " << std::time(NULL) - start_time << "seconds." << std::endl;
        
        assert(offset_ctr == num_entries*entry_len);

        uint64_t post_csum = 0;
        for (uint64_t i = 0; i < num_entries; i++)
        {
        	post_csum += hash_with_len(output_buff->data + i*output_buff->entry_len, output_buff->entry_len);
        }
        assert(csum == post_csum);
        

        for (uint32_t i = 0; i < num_bucket_sets; i++)
        {
          delete bucket_set_lens[i];
        }
        for (uint32_t i = 0; i < num_threads; i++)
        {
          delete(sort_buckets[i]);
        }
        return output_buff;
    }

}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
