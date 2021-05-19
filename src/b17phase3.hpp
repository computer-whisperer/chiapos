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

#ifndef SRC_CPP_B17PHASE3_HPP_
#define SRC_CPP_B17PHASE3_HPP_

#include "disk.hpp"
#include "radixsort.hpp"
#include "buffers.hpp"
#include "encoding.hpp"
#include "entry_sizes.hpp"
#include "exceptions.hpp"
#include "pos_constants.hpp"
//#include "phase3.hpp"

// Results of phase 3. These are passed into Phase 4, so the checkpoint tables
// can be properly built.
struct b17Phase3Results {
    // Pointers to each table start byet in the final file
    std::vector<uint64_t> final_table_begin_pointers;
    // Number of entries written for f7
    uint64_t final_entries_written;
    uint32_t right_entry_size_bits;

    uint32_t header_size;
    std::vector<Buffer*> phase3_buffers;
  //  std::unique_ptr<b17SortManager> table7_sm;
};

// This writes a number of entries into a file, in the final, optimized format. The park
// contains a checkpoint value (which is a 2k bits line point), as well as EPP (entries per
// park) entries. These entries are each divided into stub and delta section. The stub bits are
// encoded as is, but the delta bits are optimized into a variable encoding scheme. Since we
// have many entries in each park, we can approximate how much space each park with take. Format
// is: [2k bits of first_line_point]  [EPP-1 stubs] [Deltas size] [EPP-1 deltas]....
// [first_line_point] ...
void WriteParkToFile(
    FileDisk &final_disk,
    uint64_t table_start,
    uint64_t park_index,
    uint32_t park_size_bytes,
    uint128_t first_line_point,
    const std::vector<uint8_t> &park_deltas,
    const std::vector<uint64_t> &park_stubs,
    uint8_t k,
    uint8_t table_index,
    uint8_t *park_buffer,
    uint64_t const park_buffer_size)
{
    // Parks are fixed size, so we know where to start writing. The deltas will not go over
    // into the next park.
    uint64_t writer = table_start + park_index * park_size_bytes;
    uint8_t *index = park_buffer;

    first_line_point <<= 128 - 2 * k;
    Util::IntTo16Bytes(index, first_line_point);
    index += EntrySizes::CalculateLinePointSize(k);

    // We use ParkBits instead of Bits since it allows storing more data
    ParkBits park_stubs_bits;
    for (uint64_t stub : park_stubs) {
        park_stubs_bits.AppendValue(stub, (k - kStubMinusBits));
    }
    uint32_t stubs_size = EntrySizes::CalculateStubsSize(k);
    uint32_t stubs_valid_size = cdiv(park_stubs_bits.GetSize(), 8);
    park_stubs_bits.ToBytes(index);
    memset(index + stubs_valid_size, 0, stubs_size - stubs_valid_size);
    index += stubs_size;

    // The stubs are random so they don't need encoding. But deltas are more likely to
    // be small, so we can compress them
    double R = kRValues[table_index - 1];
    uint8_t *deltas_start = index + 2;
    size_t deltas_size = Encoding::ANSEncodeDeltas(park_deltas, R, deltas_start);

    if (!deltas_size) {
        // Uncompressed
        deltas_size = park_deltas.size();
        Util::IntToTwoBytesLE(index, deltas_size | 0x8000);
        memcpy(deltas_start, park_deltas.data(), deltas_size);
    } else {
        // Compressed
        Util::IntToTwoBytesLE(index, deltas_size);
    }

    index += 2 + deltas_size;

    if ((uint32_t)(index - park_buffer) > park_buffer_size) {
        std::cout << "index-park_buffer " << index - park_buffer << " park_buffer_size "
                  << park_buffer_size << std::endl;
        throw InvalidStateException(
            "Overflowed park buffer, writing " + std::to_string(index - park_buffer) +
            " bytes. Space: " + std::to_string(park_buffer_size));
    }
    assert(park_size_bytes > (index - park_buffer));
    memset(index, 0x00, park_size_bytes - (index - park_buffer));

    final_disk.Write(writer, (uint8_t *)park_buffer, park_size_bytes);
}

// Compresses the plot file tables into the final file. In order to do this, entries must be
// reorganized from the (pos, offset) bucket sorting order, to a more free line_point sorting
// order. In (pos, offset ordering), we store two pointers two the previous table, (x, y) which
// are very close together, by storing  (x, y-x), or (pos, offset), which can be done in about k
// + 8 bits, since y is in the next bucket as x. In order to decrease this, We store the actual
// entries from the previous table (e1, e2), instead of pos, offset pointers, and sort the
// entire table by (e1,e2). Then, the deltas between each (e1, e2) can be stored, which require
// around k bits.

// Converting into this format requires a few passes and sorts on disk. It also assumes that the
// backpropagation step happened, so there will be no more dropped entries. See the design
// document for more details on the algorithm.
b17Phase3Results b17RunPhase3(
    //uint8_t *memory,
    uint8_t k,
    FileDisk &tmp2_disk /*filename*/,
    std::vector<Buffer*> phase2_buffers /*plot_filename*/,
    //std::vector<uint64_t> table_sizes,
    const uint8_t *id,
    //const std::string &tmp_dirname,
    //const std::string &filename,
    uint32_t header_size,
    uint64_t memory_size,
    //uint32_t num_buckets,
    //uint32_t log_num_buckets,
    const bool show_progress,
	const uint32_t thread_num)
{
    uint8_t pos_size = k;
    uint8_t line_point_size = 2 * k - 1;

    std::vector<uint64_t> final_table_begin_pointers(12, 0);
    final_table_begin_pointers[1] = header_size;

    uint8_t table_pointer_bytes[8];
    Util::IntToEightBytes(table_pointer_bytes, final_table_begin_pointers[1]);
    tmp2_disk.Write(header_size - 10 * 8, table_pointer_bytes, 8);

    uint64_t final_entries_written = 0;
    uint32_t right_entry_size_bytes = 0;

    Buffer * r_temp_buffer = NULL;
    Buffer * l_temp_buffer = NULL;

    std::vector<Buffer *> phase3_buffers(7);
    phase3_buffers[0] = phase2_buffers[0];


    // These variables are used in the WriteParkToFile method. They are preallocatted here
    // to save time.
    uint64_t park_buffer_size = EntrySizes::CalculateLinePointSize(k) +
                                EntrySizes::CalculateStubsSize(k) + 2 +
                                EntrySizes::CalculateMaxDeltasSize(k, 1);
    uint8_t *park_buffer = new uint8_t[park_buffer_size];


    // Iterates through all tables, starting at 1, with L and R pointers.
    // For each table, R entries are rewritten with line points. Then, the right table is
    // sorted by line_point. After this, the right table entries are rewritten as (sort_key,
    // new_pos), where new_pos is the position in the table, where it's sorted by line_point,
    // and the line_points are written to disk to a final table. Finally, table_i is sorted by
    // sort_key. This allows us to compare to the next table.
    for (int table_index = 1; table_index < 7; table_index++) {
        Timer table_timer;
        Timer computation_pass_1_timer;
        std::cout << "Compressing tables " << table_index << " and " << (table_index + 1)
                  << std::endl;

        // The park size must be constant, for simplicity, but must be big enough to store EPP
        // entries. entry deltas are encoded with variable length, and thus there is no
        // guarantee that they won't override into the next park. It is only different (larger)
        // for table 1
        uint32_t park_size_bytes = EntrySizes::CalculateParkSize(k, table_index);

        // Sort key for table 7 is just y, which is k bits. For all other tables it can
        // be higher than 2^k and therefore k+1 bits are used.
        uint32_t right_sort_key_size = k;

        //uint32_t left_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, false);
        //right_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index + 1, false);

        uint64_t left_reader_count = 0;
        uint64_t right_reader_count = 0;

        r_temp_buffer = new Buffer(EntrySizes::GetMaxEntrySize(k, table_index+1, false)*phase2_buffers[table_index]->Count());
        r_temp_buffer->entry_len = EntrySizes::GetMaxEntrySize(k, table_index+1, false);

       // if (table_index > 1) {
       //     L_sort_manager->ChangeMemory(memory, sort_manager_buf_size);
       // }
/*
        R_sort_manager = std::make_unique<b17SortManager>(
            right_writer_buf,
            right_writer_buf_size,
            num_buckets,
            log_num_buckets,
            right_entry_size_bytes,
            tmp_dirname,
            filename + ".p3.t" + std::to_string(table_index + 1),
            0,
            0);*/


        bool should_read_entry = true;
        std::vector<uint64_t> left_new_pos(kCachedPositionsSize);

        uint64_t old_sort_keys[kReadMinusWrite][kMaxMatchesSingleEntry];
        uint64_t old_offsets[kReadMinusWrite][kMaxMatchesSingleEntry];
        uint16_t old_counters[kReadMinusWrite];
        for (uint16_t &old_counter : old_counters) {
            old_counter = 0;
        }
        bool end_of_right_table = false;
        uint64_t current_pos = 0;
        uint64_t end_of_table_pos = 0;
        uint64_t greatest_pos = 0;

        uint8_t *right_entry_buf;
        uint8_t *left_entry_disk_buf;
        //uint8_t *left_entry_buf_sm = new uint8_t[left_entry_size_bytes];

        uint64_t entry_sort_key, entry_pos, entry_offset;
        uint64_t cached_entry_sort_key = 0;
        uint64_t cached_entry_pos = 0;
        uint64_t cached_entry_offset = 0;

        // Similar algorithm as Backprop, to read both L and R tables simultaneously
        while (!end_of_right_table || (current_pos - end_of_table_pos <= kReadMinusWrite)) {
            old_counters[current_pos % kReadMinusWrite] = 0;

            if (end_of_right_table || current_pos <= greatest_pos) {
                while (!end_of_right_table) {
                    if (should_read_entry) {
                        if (right_reader_count == phase2_buffers[table_index]->Count()) {
                            end_of_right_table = true;
                            end_of_table_pos = current_pos;
                            break;
                        }
                        // The right entries are in the format from backprop, (sort_key, pos,
                        // offset)
                        right_entry_buf = phase2_buffers[table_index]->data + phase2_buffers[table_index]->entry_len*right_reader_count;
                        right_reader_count++;

                        entry_sort_key =
                            Util::SliceInt64FromBytes(right_entry_buf, 0, right_sort_key_size);
                        entry_pos = Util::SliceInt64FromBytes(
                            right_entry_buf, right_sort_key_size, pos_size);
                        entry_offset = Util::SliceInt64FromBytes(
                            right_entry_buf, right_sort_key_size + pos_size, kOffsetSize);
                    } else if (cached_entry_pos == current_pos) {
                        entry_sort_key = cached_entry_sort_key;
                        entry_pos = cached_entry_pos;
                        entry_offset = cached_entry_offset;
                    } else {
                    	assert(cached_entry_pos > current_pos);
                        break;
                    }

                    should_read_entry = true;

                    if (entry_pos + entry_offset > greatest_pos) {
                        greatest_pos = entry_pos + entry_offset;
                    }
                    if (entry_pos == current_pos) {
                        uint64_t old_write_pos = entry_pos % kReadMinusWrite;
                        old_sort_keys[old_write_pos][old_counters[old_write_pos]] = entry_sort_key;
                        old_offsets[old_write_pos][old_counters[old_write_pos]] =
                            (entry_pos + entry_offset);
                        ++old_counters[old_write_pos];
                    } else {
                        should_read_entry = false;
                        cached_entry_sort_key = entry_sort_key;
                        cached_entry_pos = entry_pos;
                        cached_entry_offset = entry_offset;
                        break;
                    }
                }
                if (left_reader_count < phase3_buffers[table_index-1]->Count()) {
                    // The left entries are in the new format: (sort_key, new_pos), except for table
                    // 1: (y, x).
                    left_entry_disk_buf = phase3_buffers[table_index-1]->data + left_reader_count*phase3_buffers[table_index-1]->entry_len;
                    left_reader_count++;
                }


                uint8_t tbuff[32];
                memcpy(tbuff, left_entry_disk_buf, phase3_buffers[table_index-1]->entry_len);
                // We read the "new_pos" from the L table, which for table 1 is just x. For
                // other tables, the new_pos
                if (table_index == 1) {
                    // Only k bits, since this is x
                    left_new_pos[current_pos % kCachedPositionsSize] =
                        Util::SliceInt64FromBytes(tbuff, 0, k);
                } else {
                    // k+1 bits in case it overflows
                    left_new_pos[current_pos % kCachedPositionsSize] =
                        Util::SliceInt64FromBytes(tbuff, right_sort_key_size, k);
                }
            }

            uint64_t write_pointer_pos = current_pos - kReadMinusWrite + 1;

            // Rewrites each right entry as (line_point, sort_key)
            if (current_pos + 1 >= kReadMinusWrite) {
                uint64_t left_new_pos_1 = left_new_pos[write_pointer_pos % kCachedPositionsSize];
                for (uint32_t counter = 0;
                     counter < old_counters[write_pointer_pos % kReadMinusWrite];
                     counter++) {
                    uint64_t left_new_pos_2 = left_new_pos
                        [old_offsets[write_pointer_pos % kReadMinusWrite][counter] %
                         kCachedPositionsSize];

                    // A line point is an encoding of two k bit values into one 2k bit value.
                    uint128_t line_point =
                        Encoding::SquareToLinePoint(left_new_pos_1, left_new_pos_2);

                    if (left_new_pos_1 > ((uint64_t)1 << k) ||
                        left_new_pos_2 > ((uint64_t)1 << k)) {
                        std::cout << "left or right positions too large" << std::endl;
                        std::cout << (line_point > ((uint128_t)1 << (2 * k)));
                        if ((line_point > ((uint128_t)1 << (2 * k)))) {
                            std::cout << "L, R: " << left_new_pos_1 << " " << left_new_pos_2
                                      << std::endl;
                            std::cout << "Line point: " << line_point << std::endl;
                            abort();
                        }
                    }
                    Bits to_write = Bits(line_point, line_point_size);
                    to_write += Bits(
                        old_sort_keys[write_pointer_pos % kReadMinusWrite][counter],
                        right_sort_key_size);
                    r_temp_buffer->PushEntry(to_write);
                }
            }
            current_pos += 1;
        }
        computation_pass_1_timer.PrintElapsed("\tFirst computation pass time:");

        // Remove no longer needed file
        //tmp_1_disks[table_index].Truncate(0);
        if (table_index != 1)
        {
        	delete phase2_buffers[table_index-1];
        }

        // Flush cache so all entries are written to buckets
        //R_sort_manager->FlushCache();


        Timer computation_pass_2_timer;

        right_reader_count = 0;
        uint64_t final_table_writer = final_table_begin_pointers[table_index];

        final_entries_written = 0;

        r_temp_buffer = RadixSort::SortToMemory(r_temp_buffer, 0, thread_num);

      /*  if (table_index > 1) {
            // Make sure all files are removed
            L_sort_manager.reset();
        }*/

        // L sort manager will be used for the writer, and R sort manager will be used for the
        // reader
        //R_sort_manager->ChangeMemory(right_reader_buf, right_reader_buf_size);
        /*L_sort_manager = std::make_unique<b17SortManager>(
            right_writer_buf,
            right_writer_buf_size,
            num_buckets,
            log_num_buckets,
            right_entry_size_bytes,
            tmp_dirname,
            filename + ".p3s.t" + std::to_string(table_index + 1),
            0,
            0);*/
        l_temp_buffer = new Buffer(EntrySizes::GetMaxEntrySize(k, table_index + 1, false)*r_temp_buffer->Count());
        l_temp_buffer->entry_len = EntrySizes::GetMaxEntrySize(k, table_index + 1, false);

        std::vector<uint8_t> park_deltas;
        std::vector<uint64_t> park_stubs;
        uint128_t checkpoint_line_point = 0;
        uint128_t last_line_point = 0;
        uint64_t park_index = 0;

        uint8_t *right_reader_entry_buf;

        // Now we will write on of the final tables, since we have a table sorted by line point.
        // The final table will simply store the deltas between each line_point, in fixed space
        // groups(parks), with a checkpoint in each group.
        Bits right_entry_bits;
        uint8_t index_size = table_index == 6 ? k + 1 : k;
        for (uint64_t index = 0; index < r_temp_buffer->Count(); index++) {
        	right_reader_entry_buf = r_temp_buffer->data + index*r_temp_buffer->entry_len;

            // Right entry is read as (line_point, sort_key)
            uint128_t line_point = Util::SliceInt128FromBytes(right_reader_entry_buf, 0, line_point_size);
            uint64_t sort_key =
                Util::SliceInt64FromBytes(right_reader_entry_buf, line_point_size, right_sort_key_size);

            // Write the new position (index) and the sort key
            Bits to_write = Bits(sort_key, right_sort_key_size);
            to_write += Bits(index, index_size);
            l_temp_buffer->PushEntry(to_write);

            //to_write.ToBytes(phase2_buffers[table_index]->data + phase2_buffers[table_index]->GetInsertionOffset(to_write.GetSize()/8));
            //L_sort_manager->AddToCache(to_write);

            // Every EPP entries, writes a park
            if (index % kEntriesPerPark == 0) {
                if (index != 0) {
                    WriteParkToFile(
                        tmp2_disk,
                        final_table_begin_pointers[table_index],
                        park_index,
                        park_size_bytes,
                        checkpoint_line_point,
                        park_deltas,
                        park_stubs,
                        k,
                        table_index,
                        park_buffer,
                        park_buffer_size);
                    park_index += 1;
                    final_entries_written += (park_stubs.size() + 1);
                }
                park_deltas.clear();
                park_stubs.clear();

                checkpoint_line_point = line_point;
            }
            uint128_t big_delta = line_point - last_line_point;

            // Since we have approx 2^k line_points between 0 and 2^2k, the average
            // space between them when sorted, is k bits. Much more efficient than storing each
            // line point. This is diveded into the stub and delta. The stub is the least
            // significant (k-kMinusStubs) bits, and largely random/incompressible. The small
            // delta is the rest, which can be efficiently encoded since it's usually very
            // small.

            uint64_t stub = big_delta & ((1ULL << (k - kStubMinusBits)) - 1);
            uint64_t small_delta = big_delta >> (k - kStubMinusBits);

            assert(small_delta < 256);

            if ((index % kEntriesPerPark != 0)) {
                park_deltas.push_back(small_delta);
                park_stubs.push_back(stub);
            }
            last_line_point = line_point;
        }
        delete r_temp_buffer;
        phase3_buffers[table_index] = RadixSort::SortToMemory(l_temp_buffer, 0, thread_num);

        computation_pass_2_timer.PrintElapsed("\tSecond computation pass time:");

        if (park_deltas.size() > 0) {
            // Since we don't have a perfect multiple of EPP entries, this writes the last ones
            WriteParkToFile(
                tmp2_disk,
                final_table_begin_pointers[table_index],
                park_index,
                park_size_bytes,
                checkpoint_line_point,
                park_deltas,
                park_stubs,
                k,
                table_index,
                park_buffer,
                park_buffer_size);
            final_entries_written += (park_stubs.size() + 1);
        }

        Encoding::ANSFree(kRValues[table_index - 1]);
        std::cout << "\tWrote " << final_entries_written << " entries" << std::endl;

        final_table_begin_pointers[table_index + 1] =
            final_table_begin_pointers[table_index] + (park_index + 1) * park_size_bytes;

        final_table_writer = header_size - 8 * (10 - table_index);
        Util::IntToEightBytes(table_pointer_bytes, final_table_begin_pointers[table_index + 1]);
        tmp2_disk.Write(final_table_writer, (table_pointer_bytes), 8);
        final_table_writer += 8;

        table_timer.PrintElapsed("Total compress table time:");
        if (show_progress) { progress(3, table_index, 6); }
    }

    //L_sort_manager->ChangeMemory(memory, memory_size);
    delete[] park_buffer;

    // These results will be used to write table P7 and the checkpoint tables in phase 4.
    return b17Phase3Results{
        final_table_begin_pointers,
        final_entries_written,
        right_entry_size_bytes * 8,
        header_size,
    	phase3_buffers};
}

#endif  // SRC_CPP_PHASE3_HPP
