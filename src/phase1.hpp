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

#ifndef SRC_CPP_PHASE1_HPP_
#define SRC_CPP_PHASE1_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <thread>
#include <memory>
#include <mutex>

#include "chia_filesystem.hpp"

#include "radixsort.hpp"
#include "calculate_bucket.hpp"
#include "entry_sizes.hpp"
#include "exceptions.hpp"
#include "pos_constants.hpp"
#include "threading.hpp"
#include "util.hpp"
#include "progress.hpp"
#include "buffers.hpp"

struct THREADDATA {
    int index;
    uint8_t metadata_size;
    uint8_t pos_size;
};

struct GlobalData {
    uint8_t k;
    uint8_t table_index;
    atomic<uint64_t> left_writer_count;
    atomic<uint64_t> right_writer_count;
    atomic<uint64_t> matches;
    uint64_t left_writer_buf_entries;
    uint64_t left_writer;
    uint64_t right_writer;
    uint64_t stripe_size;
    uint8_t num_threads;
    std::vector<Buffer*> compressed_tables;
    Buffer* unsorted_table;
    Buffer* sorted_table;
};

GlobalData globals;

void* phase1_thread(THREADDATA* ptd)
{
    uint8_t const k = globals.k;
    //uint64_t const right_entry_size_bytes = ptd->right_entry_size_bytes;
    uint8_t const metadata_size = ptd->metadata_size;
    //uint32_t const entry_size_bytes = ptd->entry_size_bytes;
    uint8_t const pos_size = ptd->pos_size;
    //uint64_t const prevtableentries = ptd->prevtableentries;
    //uint32_t const compressed_entry_size_bytes = ptd->compressed_entry_size_bytes;
    //std::vector<FileDisk>* ptmp_1_disks = ptd->ptmp_1_disks;

    // Streams to read and right to tables. We will have handles to two tables. We will
    // read through the left table, compute matches, and evaluate f for matching entries,
    // writing results to the right table.
    uint64_t left_buf_entries = 5000 + (uint64_t)((1.1) * (globals.stripe_size));
    uint64_t right_buf_entries = 5000 + (uint64_t)((1.1) * (globals.stripe_size));
    std::unique_ptr<uint8_t[]> right_writer_buf(new uint8_t[right_buf_entries * globals.unsorted_table->entry_len + 7]);
    std::unique_ptr<uint8_t[]> left_writer_buf(new uint8_t[left_buf_entries * globals.compressed_tables[globals.table_index]->entry_len + 7]);

    FxCalculator f(k, globals.table_index + 2);

    // Stores map of old positions to new positions (positions after dropping entries from L
    // table that did not match) Map ke
    uint16_t position_map_size = 2000;

    // Should comfortably fit 2 buckets worth of items
    std::unique_ptr<uint16_t[]> L_position_map(new uint16_t[position_map_size]);
    std::unique_ptr<uint16_t[]> R_position_map(new uint16_t[position_map_size]);

    // Start at left table pos = 0 and iterate through the whole table. Note that the left table
    // will already be sorted by y
    uint64_t totalstripes = (globals.sorted_table->entry_count + globals.stripe_size - 1) / globals.stripe_size;
    uint64_t threadstripes = (totalstripes + globals.num_threads - 1) / globals.num_threads;

    for (uint64_t stripe = 0; stripe < threadstripes; stripe++) {
        uint64_t pos = (stripe * globals.num_threads + ptd->index) * globals.stripe_size;
        uint64_t const endpos = pos + globals.stripe_size + 1;  // one y value overlap
        uint64_t left_reader = pos * globals.sorted_table->entry_len;
        uint64_t left_writer_count = 0;
        uint64_t stripe_left_writer_count = 0;
        uint64_t stripe_start_correction = 0xffffffffffffffff;
        uint64_t right_writer_count = 0;
        uint64_t matches = 0;  // Total matches

        // This is a sliding window of entries, since things in bucket i can match with things in
        // bucket
        // i + 1. At the end of each bucket, we find matches between the two previous buckets.
        std::vector<PlotEntry> bucket_L;
        std::vector<PlotEntry> bucket_R;

        uint64_t bucket = 0;
        bool end_of_table = false;  // We finished all entries in the left table

        uint64_t ignorebucket = 0xffffffffffffffff;
        bool bMatch = false;
        bool bFirstStripeOvertimePair = false;
        bool bSecondStripOvertimePair = false;
        bool bThirdStripeOvertimePair = false;

        bool bStripePregamePair = false;
        bool bStripeStartPair = false;


        uint64_t L_position_base = 0;
        uint64_t R_position_base = 0;
        uint64_t newlpos = 0;
        uint64_t newrpos = 0;
        std::vector<std::tuple<PlotEntry, PlotEntry, std::pair<Bits, Bits>>>
            current_entries_to_write;
        std::vector<std::tuple<PlotEntry, PlotEntry, std::pair<Bits, Bits>>>
            future_entries_to_write;
        std::vector<PlotEntry*> not_dropped;  // Pointers are stored to avoid copying entries

        if (pos == 0) {
            bMatch = true;
            bStripePregamePair = true;
            bStripeStartPair = true;
            stripe_left_writer_count = 0;
            stripe_start_correction = 0;
        }
/*
        Sem::Wait(ptd->theirs);
        need_new_bucket = globals.L_sort_manager->CloseToNewBucket(left_reader);
        if (need_new_bucket) {
            if (!first_thread) {
                Sem::Wait(ptd->theirs);
            }
            globals.L_sort_manager->TriggerNewBucket(left_reader);
        }
        if (!last_thread) {
            // Do not post if we are the last thread, because first thread has already
            // waited for us to finish when it starts
            Sem::Post(ptd->mine);
        }
*/
        while (pos < globals.sorted_table->entry_count + 1) {
            PlotEntry left_entry = PlotEntry();
            if (pos >= globals.sorted_table->entry_count) {
                end_of_table = true;
                left_entry.y = 0;
                left_entry.left_metadata = 0;
                left_entry.right_metadata = 0;
                left_entry.used = false;
            } else {
                // Reads a left entry from disk
                uint8_t* left_buf = globals.sorted_table->data + left_reader;
                left_reader += globals.sorted_table->entry_len;

                left_entry.y = 0;
                left_entry.read_posoffset = 0;
                left_entry.left_metadata = 0;
                left_entry.right_metadata = 0;

                uint32_t const ysize = (globals.table_index == 6) ? k : k + kExtraBits;

                if (globals.table_index == 0) {
                    // For table 1, we only have y and metadata
                    left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, k + kExtraBits);
                    left_entry.left_metadata =
                        Util::SliceInt64FromBytes(left_buf, k + kExtraBits, metadata_size);
                } else {
                    // For tables 2-6, we we also have pos and offset. We need to read this because
                    // this entry will be written again to the table without the y (and some entries
                    // are dropped).
                    left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, ysize);
                    left_entry.read_posoffset =
                        Util::SliceInt64FromBytes(left_buf, ysize, pos_size + kOffsetSize);
                    if (metadata_size <= 128) {
                        left_entry.left_metadata =
                            Util::SliceInt128FromBytes(left_buf, ysize + pos_size + kOffsetSize, metadata_size);
                    } else {
                        // Large metadatas that don't fit into 128 bits. (k > 32).
                        left_entry.left_metadata =
                            Util::SliceInt128FromBytes(left_buf, ysize + pos_size + kOffsetSize, 128);
                        left_entry.right_metadata = Util::SliceInt128FromBytes(
                            left_buf, ysize + pos_size + kOffsetSize + 128, metadata_size - 128);
                    }
                }
            }

            // This is not the pos that was read from disk,but the position of the entry we read,
            // within L table.
            left_entry.pos = pos;
            left_entry.used = false;
            uint64_t y_bucket = left_entry.y / kBC;

            if (!bMatch) {
                if (ignorebucket == 0xffffffffffffffff) {
                    ignorebucket = y_bucket;
                } else {
                    if ((y_bucket != ignorebucket)) {
                        bucket = y_bucket;
                        bMatch = true;
                    }
                }
            }
            if (!bMatch) {
                stripe_left_writer_count++;
                R_position_base = stripe_left_writer_count;
                pos++;
                continue;
            }

            // Keep reading left entries into bucket_L and R, until we run out of things
            if (y_bucket == bucket) {
                bucket_L.emplace_back(left_entry);
            } else if (y_bucket == bucket + 1) {
                bucket_R.emplace_back(left_entry);
            } else {
                // cout << "matching! " << bucket << " and " << bucket + 1 << endl;
                // This is reached when we have finished adding stuff to bucket_R and bucket_L,
                // so now we can compare entries in both buckets to find matches. If two entries
                // match, match, the result is written to the right table. However the writing
                // happens in the next iteration of the loop, since we need to remap positions.
                uint16_t idx_L[10000];
                uint16_t idx_R[10000];
                int32_t idx_count=0;

                if (!bucket_L.empty()) {
                    not_dropped.clear();

                    if (!bucket_R.empty()) {
                        // Compute all matches between the two buckets and save indeces.
                        idx_count = f.FindMatches(bucket_L, bucket_R, idx_L, idx_R);
                        if(idx_count >= 10000) {
                            std::cout << "sanity check: idx_count exceeded 10000!" << std::endl;
                            exit(0);
                        }
                        // We mark entries as used if they took part in a match.
                        for (int32_t i=0; i < idx_count; i++) {
                            bucket_L[idx_L[i]].used = true;
                            if (end_of_table) {
                                bucket_R[idx_R[i]].used = true;
                            }
                        }
                    }

                    // Adds L_bucket entries that are used to not_dropped. They are used if they
                    // either matched with something to the left (in the previous iteration), or
                    // matched with something in bucket_R (in this iteration).
                    for (size_t bucket_index = 0; bucket_index < bucket_L.size(); bucket_index++) {
                        PlotEntry& L_entry = bucket_L[bucket_index];
                        if (L_entry.used) {
                            not_dropped.emplace_back(&bucket_L[bucket_index]);
                        }
                    }
                    if (end_of_table) {
                        // In the last two buckets, we will not get a chance to enter the next
                        // iteration due to breaking from loop. Therefore to write the final
                        // bucket in this iteration, we have to add the R entries to the
                        // not_dropped list.
                        for (size_t bucket_index = 0; bucket_index < bucket_R.size();
                             bucket_index++) {
                            PlotEntry& R_entry = bucket_R[bucket_index];
                            if (R_entry.used) {
                                not_dropped.emplace_back(&R_entry);
                            }
                        }
                    }
                    // We keep maps from old positions to new positions. We only need two maps,
                    // one for L bucket and one for R bucket, and we cycle through them. Map
                    // keys are stored as positions % 2^10 for efficiency. Map values are stored
                    // as offsets from the base position for that bucket, for efficiency.
                    std::swap(L_position_map, R_position_map);
                    L_position_base = R_position_base;
                    R_position_base = stripe_left_writer_count;

                    for (PlotEntry*& entry : not_dropped) {
                        // The new position for this entry = the total amount of thing written
                        // to L so far. Since we only write entries in not_dropped, about 14% of
                        // entries are dropped.
                        R_position_map[entry->pos % position_map_size] =
                            stripe_left_writer_count - R_position_base;

                        if (bStripeStartPair) {
                            if (stripe_start_correction == 0xffffffffffffffff) {
                                stripe_start_correction = stripe_left_writer_count;
                            }

                            if (left_writer_count >= left_buf_entries) {
                                throw InvalidStateException("Left writer count overrun");
                            }
                            uint8_t* tmp_buf =
                                left_writer_buf.get() + left_writer_count * globals.compressed_tables[globals.table_index]->entry_len;

                            left_writer_count++;
                            // memset(tmp_buf, 0xff, compressed_entry_size_bytes);

                            // Rewrite left entry with just pos and offset, to reduce working space
                            uint64_t new_left_entry;
                            if (globals.table_index == 0)
                                new_left_entry = entry->left_metadata;
                            else
                            {
                                new_left_entry = entry->read_posoffset;
                                uint64_t pos = (new_left_entry>>kOffsetSize)%(1ULL << pos_size);
                                uint64_t offset = new_left_entry%(1ULL << kOffsetSize);
                                assert(pos + offset < globals.compressed_tables[globals.table_index-1]->entry_count);
                                assert(offset > 0);
                            }

                            new_left_entry <<= 64 - (globals.table_index == 0 ? k : pos_size + kOffsetSize);
                            Util::IntToEightBytes(tmp_buf, new_left_entry);
                        }
                        stripe_left_writer_count++;
                    }

                    // Two vectors to keep track of things from previous iteration and from this
                    // iteration.
                    current_entries_to_write = std::move(future_entries_to_write);
                    future_entries_to_write.clear();

                    for (int32_t i=0; i < idx_count; i++) {
                        PlotEntry& L_entry = bucket_L[idx_L[i]];
                        PlotEntry& R_entry = bucket_R[idx_R[i]];

                        if (bStripeStartPair)
                            matches++;

                        // Sets the R entry to used so that we don't drop in next iteration
                        R_entry.used = true;
                        // Computes the output pair (fx, new_metadata)
                        if (metadata_size <= 128) {
                            const std::pair<Bits, Bits>& f_output = f.CalculateBucket(
                                Bits(L_entry.y, k + kExtraBits),
                                Bits(L_entry.left_metadata, metadata_size),
                                Bits(R_entry.left_metadata, metadata_size));
                            future_entries_to_write.emplace_back(L_entry, R_entry, f_output);
                        } else {
                            // Metadata does not fit into 128 bits
                            const std::pair<Bits, Bits>& f_output = f.CalculateBucket(
                                Bits(L_entry.y, k + kExtraBits),
                                Bits(L_entry.left_metadata, 128) +
                                    Bits(L_entry.right_metadata, metadata_size - 128),
                                Bits(R_entry.left_metadata, 128) +
                                    Bits(R_entry.right_metadata, metadata_size - 128));
                            future_entries_to_write.emplace_back(L_entry, R_entry, f_output);
                        }
                    }

                    // At this point, future_entries_to_write contains the matches of buckets L
                    // and R, and current_entries_to_write contains the matches of L and the
                    // bucket left of L. These are the ones that we will write.
                    uint16_t final_current_entry_size = current_entries_to_write.size();
                    if (end_of_table) {
                        // For the final bucket, write the future entries now as well, since we
                        // will break from loop
                        current_entries_to_write.insert(
                            current_entries_to_write.end(),
                            future_entries_to_write.begin(),
                            future_entries_to_write.end());
                    }
                    for (size_t i = 0; i < current_entries_to_write.size(); i++) {
                        const auto& [L_entry, R_entry, f_output] = current_entries_to_write[i];

                        // We only need k instead of k + kExtraBits bits for the last table
                        Bits new_entry = (globals.table_index + 2) == 7 ? std::get<0>(f_output).Slice(0, k)
                                                              : std::get<0>(f_output);

                        // Maps the new positions. If we hit end of pos, we must write things in
                        // both final_entries to write and current_entries_to_write, which are
                        // in both position maps.
                        if (!end_of_table || i < final_current_entry_size) {
                            newlpos =
                                L_position_map[L_entry.pos % position_map_size] + L_position_base;
                        } else {
                            newlpos =
                                R_position_map[L_entry.pos % position_map_size] + R_position_base;
                        }
                        newrpos = R_position_map[R_entry.pos % position_map_size] + R_position_base;
                        // Position in the previous table
                        new_entry.AppendValue(newlpos, pos_size);

                        // Offset for matching entry
                        if (newrpos - newlpos > (1U << kOffsetSize) * 97 / 100) {
                            throw InvalidStateException(
                                "Offset too large: " + std::to_string(newrpos - newlpos));
                        }

                        int64_t offset = newrpos - newlpos;
                        assert (offset > 0);

                        new_entry.AppendValue(offset, kOffsetSize);
                        // New metadata which will be used to compute the next f
                        new_entry += std::get<1>(f_output);

                        if (right_writer_count >= right_buf_entries) {
                            throw InvalidStateException("Left writer count overrun");
                        }

                        if (bStripeStartPair) {
                            uint8_t* right_buf =
                                right_writer_buf.get() + right_writer_count * globals.unsorted_table->entry_len;
                            new_entry.ToBytes(right_buf);
                            right_writer_count++;
                        }
                    }
                }

                if (pos >= endpos) {
                    if (!bFirstStripeOvertimePair)
                        bFirstStripeOvertimePair = true;
                    else if (!bSecondStripOvertimePair)
                        bSecondStripOvertimePair = true;
                    else if (!bThirdStripeOvertimePair)
                        bThirdStripeOvertimePair = true;
                    else {
                        break;
                    }
                } else {
                    if (!bStripePregamePair)
                        bStripePregamePair = true;
                    else if (!bStripeStartPair)
                        bStripeStartPair = true;
                }

                if (y_bucket == bucket + 2) {
                    // We saw a bucket that is 2 more than the current, so we just set L = R, and R
                    // = [entry]
                    bucket_L = std::move(bucket_R);
                    bucket_R.clear();
                    bucket_R.emplace_back(std::move(left_entry));
                    ++bucket;
                } else {
                    // We saw a bucket that >2 more than the current, so we just set L = [entry],
                    // and R = []
                    bucket = y_bucket;
                    bucket_L.clear();
                    bucket_L.emplace_back(std::move(left_entry));
                    bucket_R.clear();
                }
            }
            // Increase the read pointer in the left table, by one
            ++pos;
        }

        // If we needed new bucket, we already waited
        // Do not wait if we are the first thread, since we are guaranteed that everything is written
        /*
        if (!need_new_bucket && !first_thread) {
            Sem::Wait(ptd->theirs);
        }*/
        uint64_t offsetl = globals.compressed_tables[globals.table_index]->GetInsertionOffset(left_writer_count * globals.compressed_tables[globals.table_index]->entry_len);

        uint32_t const ysize = (globals.table_index + 2 == 7) ? k : k + kExtraBits;
        uint32_t const startbyte = ysize / 8;
        uint32_t const endbyte = (ysize + pos_size + 7) / 8 - 1;
        uint64_t const shiftamt = (8 - ((ysize + pos_size) % 8)) % 8;
        uint64_t const correction = (offsetl/globals.compressed_tables[globals.table_index]->entry_len - stripe_start_correction) << shiftamt;

        // Correct positions
        for (uint32_t i = 0; i < right_writer_count; i++) {
            uint64_t posaccum = 0;
            uint8_t* entrybuf = right_writer_buf.get() + i * globals.unsorted_table->entry_len;

            for (uint32_t j = startbyte; j <= endbyte; j++) {
                posaccum = (posaccum << 8) | (entrybuf[j]);
            }
            posaccum += correction;
            for (uint32_t j = endbyte; j >= startbyte; --j) {
                entrybuf[j] = posaccum & 0xff;
                posaccum = posaccum >> 8;
            }
        }
     //   if (globals.table_index < 6) {
            for (uint64_t i = 0; i < right_writer_count; i++) {
                uint64_t roffset = globals.unsorted_table->GetInsertionOffset(globals.unsorted_table->entry_len);
                memcpy(globals.unsorted_table->data + roffset, right_writer_buf.get() + i * globals.unsorted_table->entry_len, globals.unsorted_table->entry_len);
            }
       /* } else {
            // Writes out the right table for table 7
            (*ptmp_1_disks)[globals.table_index + 1].Write(
                globals.right_writer,
                right_writer_buf.get(),
                right_writer_count * right_entry_size_bytes);
        }*/
        globals.right_writer += right_writer_count * globals.unsorted_table->entry_len;
        globals.right_writer_count += right_writer_count;

        memcpy(globals.compressed_tables[globals.table_index]->data + offsetl, left_writer_buf.get(), left_writer_count * globals.compressed_tables[globals.table_index]->entry_len);

        globals.left_writer += left_writer_count * globals.compressed_tables[globals.table_index]->entry_len;
        globals.left_writer_count += left_writer_count;

        globals.matches += matches;
      //  Sem::Post(ptd->mine);
    }

    return 0;
}

void* F1thread(int const index, const uint8_t* id)
{
    uint8_t const k = globals.k;
    uint32_t const entry_size_bytes = 16;
    uint64_t const max_value = ((uint64_t)1 << (k));
    uint64_t const right_buf_entries = 1 << (kBatchSizes);

    std::unique_ptr<uint64_t[]> f1_entries(new uint64_t[(1U << kBatchSizes)]);

    F1Calculator f1(k, id);

    std::unique_ptr<uint8_t[]> right_writer_buf(new uint8_t[right_buf_entries * entry_size_bytes]);

    // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
    // to increase CPU efficency.
    for (uint64_t lp = index; lp <= (((uint64_t)1) << (k - kBatchSizes));
         lp = lp + globals.num_threads)
    {
        // For each pair x, y in the batch

        uint64_t right_writer_count = 0;
        uint64_t x = lp * (1 << (kBatchSizes));

        uint64_t const loopcount = std::min(max_value - x, (uint64_t)1 << (kBatchSizes));

        // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
        // to increase CPU efficency.
        f1.CalculateBuckets(x, loopcount, f1_entries.get());
        for (uint32_t i = 0; i < loopcount; i++) {
            uint128_t entry;

            entry = (uint128_t)f1_entries[i] << (128 - kExtraBits - k);
            entry |= (uint128_t)x << (128 - kExtraBits - 2 * k);
            Util::IntTo16Bytes(globals.unsorted_table->data + x*entry_size_bytes, entry);
            right_writer_count++;
            x++;
        }
    }

    return 0;
}

void assert_matching(uint64_t lout, uint64_t rout)
{

    uint64_t bucket_id_lout = lout/kBC;
    uint64_t bucket_id_rout = rout/kBC;
    assert(bucket_id_lout + 1 == bucket_id_rout);
    uint64_t b_id_l = (lout%kBC)/kC;
    uint64_t c_id_l = (lout%kBC)%kC;
    uint64_t b_id_r = (rout%kBC)/kC;
    uint64_t c_id_r = (rout%kBC)%kC;
    uint64_t m = (b_id_r - b_id_l)%kB;
    assert(c_id_r - c_id_l == (2*m + (bucket_id_lout%2))*(2*m + (bucket_id_lout%2)));
}

// This is Phase 1, or forward propagation. During this phase, all of the 7 tables,
// and f functions, are evaluated. The result is an intermediate plot file, that is
// several times larger than what the final file will be, but that has all of the
// proofs of space in it. First, F1 is computed, which is special since it uses
// ChaCha8, and each encryption provides multiple output values. Then, the rest of the
// f functions are computed, and a sort on disk happens for each table.
vector<Buffer*> RunPhase1(
    uint8_t const k,
    uint8_t const* id,
    uint32_t const stripe_size,
    uint8_t const num_threads,
    bool const enable_bitfield,
    bool const show_progress)
{
  
    F1Calculator f1(k, id);
    
    std::cout << "Computing table 1" << std::endl;
    globals.stripe_size = stripe_size;
    globals.num_threads = num_threads;
    Timer f1_start_time;
    uint64_t x = 0;

    globals.k = k;
    globals.table_index = 0;
    globals.unsorted_table = new Buffer(16*(1ULL<<k));
    globals.unsorted_table->entry_len = 16; //EntrySizes::GetMaxEntrySize(k, 1, true);
    globals.unsorted_table->entry_count = 1ULL<<k;
    globals.compressed_tables.resize(7);
    /*
    globals.L_sort_manager = std::make_unique<SortManager>(
        memory_size,
        num_buckets,
        log_num_buckets,
        t1_entry_size_bytes,
        tmp_dirname,
        filename + ".p1.t1",
        0,
        globals.stripe_size);
        */

    // These are used for sorting on disk. The sort on disk code needs to know how
    // many elements are in each bucket.
    /*
    std::vector<uint64_t> table_sizes = std::vector<uint64_t>(8, 0);

    std::mutex sort_manager_mutex;
    */

    {
        // Start of parallel execution
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back(F1thread, i, id);
        }

        for (auto& t : threads) {
            t.join();
        }
        // end of parallel execution
    }

    f1_start_time.PrintElapsed("F1 complete, time:");
    /*
    globals.L_sort_manager->FlushCache();
    
    table_sizes[1] = x + 1;
    * */

    // Store positions to previous tables, in k bits.
    uint8_t pos_size = k;

    // For tables 1 through 6, sort the table, calculate matches, and write
    // the next table. This is the left table index.
    for (uint8_t table_index = 0; table_index < 6; table_index++) {

        uint8_t const metadata_size = kVectorLens[table_index+2] * k;
        
        // Sort the previous table
        globals.sorted_table = RadixSort::SortToMemory(
                          globals.unsorted_table,
                          0, 
                          globals.num_threads);




        // May be up to this large, will probably be lower in reality, in which case the pages won't actually get allocated
        globals.unsorted_table = new Buffer(globals.sorted_table->entry_count*EntrySizes::GetMaxEntrySize(k, table_index + 1+1, true)*1.2);
        globals.unsorted_table->entry_len = EntrySizes::GetMaxEntrySize(k, table_index + 1+1, true);
        // May be up to this large, will probably be lower in reality, in which case the pages won't actually get allocated
        globals.compressed_tables[table_index] = new Buffer(EntrySizes::GetMaxEntrySize(k, table_index+1, false)*globals.sorted_table->entry_count);
        globals.compressed_tables[table_index]->entry_len = EntrySizes::GetMaxEntrySize(k, table_index+1, false);
        assert(globals.compressed_tables[table_index]->entry_len > 0);
        assert(globals.unsorted_table->entry_len > 0);

        Timer table_timer;
        if (enable_bitfield && table_index != 0) 
        {
            // We only write pos and offset to tables 2-6 after removing
            // metadata
            globals.compressed_tables[table_index]->entry_len = cdiv(k + kOffsetSize, 8);
            if (table_index == 5) 
            {
                // Table 7 will contain f7, pos and offset
                globals.unsorted_table->entry_len = EntrySizes::GetKeyPosOffsetSize(k);
            }
        }

        std::cout << "Computing table " << int{table_index + 1+1} << std::endl;
        // Start of parallel execution

        FxCalculator f(k, table_index + 1 + 1);  // dummy to load static table

        globals.matches = 0;
        globals.left_writer_count = 0;
        globals.right_writer_count = 0;
        globals.right_writer = 0;
        globals.left_writer = 0;
        globals.table_index = table_index;

/*
        globals.R_sort_manager = std::make_unique<SortManager>(
            memory_size,
            num_buckets,
            log_num_buckets,
            right_entry_size_bytes,
            tmp_dirname,
            filename + ".p1.t" + std::to_string(table_index + 1),
            0,
            globals.stripe_size);
            * 

        globals.L_sort_manager->TriggerNewBucket(0);
        * */
        

        Timer computation_pass_timer;

        auto td = std::make_unique<THREADDATA[]>(num_threads);

        std::vector<std::thread> threads;

        F1Calculator f1(k, id);
        for (int i = 0; i < num_threads; i++) {
            td[i].index = i;

            td[i].metadata_size = metadata_size;
            td[i].pos_size = pos_size;
            
            threads.emplace_back(phase1_thread, &td[i]);
        }

        for (auto& t : threads) {
            t.join();
        }
        
        // end of parallel execution

        globals.unsorted_table->entry_count = (*globals.unsorted_table->insert_pos)/globals.unsorted_table->entry_len;
        globals.compressed_tables[table_index]->entry_count = (*globals.compressed_tables[table_index]->insert_pos)/globals.compressed_tables[table_index]->entry_len;
        assert(globals.compressed_tables[table_index]->entry_len > 0);
        assert(globals.unsorted_table->entry_len > 0);
        delete globals.sorted_table;


        // Test for zero offsets


        // Total matches found in the left table
        std::cout << "\tTotal matches: " << globals.matches << std::endl;

/*
        table_sizes[table_index] = globals.left_writer_count;
        table_sizes[table_index + 1] = globals.right_writer_count;
*/
        // Truncates the file after the final write position, deleting no longer useful
        // working space
        /*
        tmp_1_disks[table_index].Truncate(globals.left_writer);
        globals.L_sort_manager.reset();
        if (table_index < 6) {
            globals.R_sort_manager->FlushCache();
            globals.L_sort_manager = std::move(globals.R_sort_manager);
        } else {
            tmp_1_disks[table_index + 1].Truncate(globals.right_writer);
        }

*/
        // Resets variables
        
        if (globals.matches != globals.right_writer_count) {
            throw InvalidStateException(
                "Matches do not match with number of write entries " +
                std::to_string(globals.matches) + " " + std::to_string(globals.right_writer_count));
        }

        //prevtableentries = globals.right_writer_count;
        table_timer.PrintElapsed("Forward propagation table time:");
        if (show_progress) {
            progress(1, table_index, 6);
        }
    }
    //table_sizes[0] = 0;
    //globals.R_sort_manager.reset();
    

    //globals.compressed_tables[6] = RadixSort::SortToMemory(globals.unsorted_table, k, num_threads);
    globals.compressed_tables[6] = globals.unsorted_table;

    // Test that we can draw good proofs from this
    uint64_t challenge = 0x28b40d;
    challenge = challenge%(1ULL<<k);
    uint64_t i;
    cout << "Looking for : " << challenge << endl;
    for (i = 0; i < globals.compressed_tables[6]->entry_count; i++)
    {
    	uint64_t value = (*(uint64_t*)(globals.compressed_tables[6]->data+i*globals.compressed_tables[6]->entry_len));
    	value = value%(1ULL<<k);

    	if (value == challenge)
    	{
    		break;
    	}
    }
    if (i < globals.compressed_tables[6]->entry_count)
    {
    	cout << "Got inputs: ";
    	vector<uint64_t> x(64);
    	for (uint32_t j = 0; j < 64; j++)
    	{
    		uint64_t next_pos = i;
    		uint32_t l;
    		for (l = 6; l > 0; l--)
    		{

    			uint8_t * entry = globals.compressed_tables[l]->data + globals.compressed_tables[l]->entry_len*next_pos;
    			uint32_t pos_offset = (l==6) ? k : 0;
    			uint32_t poslen = k;
    			uint64_t pos = Util::SliceInt64FromBytes(entry, pos_offset, poslen);
    			uint64_t offset = Util::SliceInt64FromBytes(entry, pos_offset+poslen, kOffsetSize);
    			assert(offset > 0);
    			uint8_t mask = 1ULL<<(l-1);
    			if (j & mask)
    			{
    				next_pos = pos + offset;
    			}
    			else
    			{
    				next_pos = pos;
    			}
    			assert(next_pos < globals.compressed_tables[l]->entry_count);
    		}
    		uint8_t * entry = globals.compressed_tables[l]->data + globals.compressed_tables[l]->entry_len*next_pos;
    		x[j] = Util::SliceInt64FromBytes(entry, k, k);
    		cout << x[j];
    		if (j < 63)
    		{
    			cout << ", ";
    		}
    	}
    	cout << endl;
    	// Check for matching conditions and re-combine
		vector<Bits> input_collations;
		vector<Bits> input_fs;
		for (uint32_t fi = 1; fi <= 7; fi++)
		{
			vector<Bits> output_fs;
			vector<Bits> output_collations;
			if (fi == 1)
			{
				F1Calculator f1(k, id);
        		for (uint32_t i = 0; i < x.size(); i++)
        		{
        			Bits b = Bits(x[i], k);
        			output_collations.push_back(b);
        			output_fs.push_back(f1.CalculateF(b));
        		}
			}
			else
			{
				FxCalculator fx(k, fi);
        		for (uint32_t i = 0; i < input_collations.size(); i += 2)
        		{
        			//assert_matching(input_fs[i].GetValue(), input_fs[i+1].GetValue());
        			auto out = fx.CalculateBucket(input_fs[i], input_collations[i], input_collations[i+1]);
        			output_fs.push_back(out.first);
        			output_collations.push_back(out.second);
        		}
			}
			input_collations = output_collations;
			input_fs = output_fs;
		}
		cout << "Result of tree: f7(...) = " << input_fs[0] << endl;

    }
    else
    {
    	cout << "Could not find " << challenge << " in table 7." << endl;
    }



    return globals.compressed_tables;
}

#endif  // SRC_CPP_PHASE1_HPP
