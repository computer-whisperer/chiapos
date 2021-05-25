#include "phase3_c.hpp"
#include "phase2_c.hpp"
#include "phase1_c.hpp"
#include "buffers.hpp"
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>
#include <string>
#include "util.hpp"
#include "encoding.hpp"
#include "entry_sizes.hpp"

using namespace std;


const uint64_t line_point_bits = K*2;
const uint64_t bits_in_bucket = 14;
const uint64_t bucket_denominator = 1ULL << (line_point_bits - bits_in_bucket);

const uint64_t num_buckets = 1ULL << bits_in_bucket;
const uint64_t mean_entries_in_bucket = (1ULL << K)/num_buckets;

struct BucketEntry
{
		uint64_t orig_pos : phase1_pos_size;
		uint32_t line_point : 11;
};

struct Bucket
{
	uint64_t output_base_pos;
	vector<struct BucketEntry> entries;
	mutex entriesMutex;
};

void Phase3CThreadA(vector<Buffer*>& phase1_buffers, vector<atomic<uint8_t>>& phase2_used_entries, atomic_long& coordinator, vector<Bucket>& buckets,  vector<uint32_t>& new_positions, uint32_t table_index)
{
	const uint64_t block_size = 1ULL<<12;
	while (1)
	{
		uint32_t block_id = coordinator.fetch_add(1);
		if (block_id*block_size > (phase1_buffers[table_index]->Count())){
			break;
		}
		uint64_t num_in_block = block_size;
		if (num_in_block > (phase1_buffers[table_index]->Count() - (block_id*block_size)))
		{
			num_in_block =  (phase1_buffers[table_index]->Count() - (block_id*block_size));
		}

		for (uint64_t i = block_id*block_size; i < block_id*block_size + num_in_block; i++)
		{
			if ((table_index < 6) && ((phase2_used_entries[i/8] & (1U << (i%8))) == 0))
			{
				continue;
			}
			void * entry = phase1_buffers[table_index]->data + phase1_buffers[table_index]->entry_len*i;
			uint64_t pos;
			uint64_t offset;
			if (table_index < 6)
			{
				pos = ((struct Phase1PosOffsetEntry *)entry)->pos;
				offset = ((struct Phase1PosOffsetEntry *)entry)->offset;
			}
			else
			{
				pos = ((struct Phase1Table7Entry *)entry)->pos;
				offset = ((struct Phase1Table7Entry *)entry)->offset;
			}

			uint64_t l_val;
			uint64_t r_val;

			if (table_index == 1)
			{
				l_val = ((struct Phase1Table1Entry *)(phase1_buffers[table_index-1]->data + phase1_buffers[table_index-1]->entry_len*pos))->x;
				r_val = ((struct Phase1Table1Entry *)(phase1_buffers[table_index-1]->data + phase1_buffers[table_index-1]->entry_len*(pos+offset)))->x;
			}
			else
			{
				l_val = new_positions[pos];
				r_val = new_positions[pos+offset];
			}

			uint128_t line_point = Encoding::SquareToLinePoint(l_val, r_val);

			struct BucketEntry new_entry;
			new_entry.orig_pos = i;
			new_entry.line_point =  line_point%bucket_denominator;
			uint32_t bucket_id = line_point/bucket_denominator;

			buckets[bucket_id].entriesMutex.lock();
			buckets[bucket_id].entries.push_back(new_entry);
			buckets[bucket_id].entriesMutex.unlock();
		}
	}
}


void Phase3CThreadB(atomic_long& coordinator, vector<Bucket>& buckets, vector<uint32_t>& new_positions)
{
	while (1)
	{
		uint32_t bucket_id = coordinator.fetch_add(1);
		if (bucket_id >= buckets.size()){
			break;
		}
	    struct {
	        bool operator()(BucketEntry a, BucketEntry b) const { return a.line_point < b.line_point; }
	    } customLess;
		sort(buckets[bucket_id].entries.begin(), buckets[bucket_id].entries.end(), customLess);


		for (uint64_t i = 0; i < buckets[bucket_id].entries.size(); i++)
		{
			new_positions[buckets[bucket_id].entries[i].orig_pos] = buckets[bucket_id].output_base_pos + i;
		}
	}
}

void Phase3CThreadC(atomic_long& coordinator, vector<Bucket>& buckets, Buffer* output_buffer, uint64_t output_table_offset, uint8_t table_index)
{
	while (1)
	{
		uint64_t park_size_bytes = EntrySizes::CalculateParkSize(K, table_index);

		uint32_t park_id = coordinator.fetch_add(1);
		// Output everything for this plot id

		uint64_t park_starting_pos = kEntriesPerPark*park_id;
		uint64_t bucket_id = 0;

		vector<uint64_t> line_points;

		while (bucket_id < buckets.size())
		{
			if (buckets[bucket_id].output_base_pos + buckets[bucket_id].entries.size() > park_starting_pos)
			{
				for (uint64_t i = park_starting_pos - buckets[bucket_id].output_base_pos; i < buckets[bucket_id].entries.size(); i++)
				{
					line_points.push_back(buckets[bucket_id].entries[i].line_point);

					if (line_points.size() == kEntriesPerPark)
					{
						break;
					}
				}
			}
			if (line_points.size() == kEntriesPerPark)
			{
				break;
			}
			bucket_id++;
		}

		if (line_points.size() == 0)
		{
			break;
		}

        // Since we have approx 2^k line_points between 0 and 2^2k, the average
        // space between them when sorted, is k bits. Much more efficient than storing each
        // line point. This is diveded into the stub and delta. The stub is the least
        // significant (k-kMinusStubs) bits, and largely random/incompressible. The small
        // delta is the rest, which can be efficiently encoded since it's usually very
        // small.

		vector<uint64_t> park_stubs;
		vector<uint8_t> park_deltas;
		for (uint32_t i = 1; i < line_points.size(); i++)
		{
			uint128_t big_delta = line_points[i] - line_points[i-1];
            uint64_t stub = big_delta & ((1ULL << (K - kStubMinusBits)) - 1);
            uint64_t small_delta = big_delta >> (K - kStubMinusBits);
            assert(small_delta < 256);
            park_deltas.push_back(small_delta);
            park_stubs.push_back(stub);
		}

	    // Parks are fixed size, so we know where to start writing. The deltas will not go over
	    // into the next park.
		uint8_t * park_start = output_buffer->data + output_table_offset + park_id * park_size_bytes;
	    uint8_t * dest = park_start;

	    *(uint64_t *)dest = line_points[0];
	    dest += (line_point_bits + 7)/8;

	    // We use ParkBits instead of Bits since it allows storing more data
	    ParkBits park_stubs_bits;
	    for (uint64_t stub : park_stubs) {
	        park_stubs_bits.AppendValue(stub, (K - kStubMinusBits));
	    }
	    uint32_t stubs_size = EntrySizes::CalculateStubsSize(K);
	    uint32_t stubs_valid_size = cdiv(park_stubs_bits.GetSize(), 8);
	    park_stubs_bits.ToBytes(dest);
	    memset(dest + stubs_valid_size, 0, stubs_size - stubs_valid_size);
	    dest += stubs_size;

	    // The stubs are random so they don't need encoding. But deltas are more likely to
	    // be small, so we can compress them
	    double R = kRValues[table_index - 1];
	    uint8_t *deltas_start = dest + 2;
	    size_t deltas_size = Encoding::ANSEncodeDeltas(park_deltas, R, deltas_start);

	    if (!deltas_size) {
	        // Uncompressed
	        deltas_size = park_deltas.size();
	        Util::IntToTwoBytesLE(dest, deltas_size | 0x8000);
	        memcpy(deltas_start, park_deltas.data(), deltas_size);
	    } else {
	        // Compressed
	        Util::IntToTwoBytesLE(dest, deltas_size);
	    }

	    dest += 2 + deltas_size;

	    assert(park_size_bytes > (uint64_t)(dest - park_start));
	}
}


struct Phase3_Out Phase3C(
		vector<Buffer*> phase1_buffers,
		vector<vector<atomic<uint8_t>>>& phase2_used_entries,
		uint32_t num_threads,
		const uint8_t* memo,
        uint32_t memo_len,
        const uint8_t* id,
        uint32_t id_len,
		string dest_fname)
{
	phase1_buffers[0]->SwapInAsync(false);
	phase1_buffers[1]->SwapInAsync(false);

	// Try to predict the final file size
	uint64_t predicted_file_size_bytes = 128;// header
	for (uint8_t table_index = 1; table_index < 7; table_index++)
	{
		uint64_t park_size_bytes = EntrySizes::CalculateParkSize(K, table_index);
		uint64_t num_parks = phase1_buffers[table_index]->Count()/kEntriesPerPark;
		predicted_file_size_bytes += park_size_bytes*num_parks;
	}

	Buffer* output_buffer = new Buffer(predicted_file_size_bytes, dest_fname);

    // 19 bytes  - "Proof of Space Plot" (utf-8)
    // 32 bytes  - unique plot id
    // 1 byte    - k
    // 2 bytes   - format description length
    // x bytes   - format description
    // 2 bytes   - memo length
    // x bytes   - memo

    output_buffer->InsertString("Proof of Space Plot");
    output_buffer->InsertData((void*)id, kIdLen);
    *(uint8_t*)(output_buffer->data + output_buffer->GetInsertionOffset(1)) = K;
    *(uint16_t*)(output_buffer->data + output_buffer->GetInsertionOffset(2)) = kFormatDescription.size();
    output_buffer->InsertString(kFormatDescription);
    *(uint16_t*)(output_buffer->data + output_buffer->GetInsertionOffset(2)) = memo_len;
    output_buffer->InsertData((void*)memo, memo_len);

    uint8_t pointers[12 * 8];
    uint32_t pointers_offset = output_buffer->InsertData(pointers, sizeof(pointers));
    uint64_t * final_table_begin_pointers = (uint64_t*)output_buffer->data + pointers_offset;

    uint64_t output_table_offset = *(output_buffer->insert_pos);

	vector<uint32_t> new_positions;
	vector<Bucket> buckets(num_buckets);
	for (auto &it : buckets)
	{
		it.entries.reserve(mean_entries_in_bucket*1.2);
	}

	for (uint32_t table_index = 1; table_index < 7; table_index++)
	{
		cout << "Starting table " << table_index << endl;
		for (auto &it : buckets)
		{
			it.entries.clear();
		}

		if (table_index < 6)
		{
			phase1_buffers[table_index+1]->SwapInAsync(false);
		}

		if (table_index == 1)
		{
			phase1_buffers[table_index-1]->WaitForSwapIn();
		}
		phase1_buffers[table_index]->WaitForSwapIn();

		cout << "Starting thread A" << endl;

		atomic_long coordinator = 0;
		vector<thread> threads;
		for (uint32_t i = 0; i < num_threads; i++)
		{
			threads.push_back(thread(Phase3CThreadA, ref(phase1_buffers), ref(phase2_used_entries[table_index]), ref(coordinator), ref(buckets), ref(new_positions), table_index));
		}

		for (auto &it: threads)
		{
			it.join();
		}

		cout << "Starting thread B" << endl;

		// Fill output_base_pos
		uint64_t ctr = 0;
		for (auto &it: buckets)
		{
			it.output_base_pos = ctr;
			ctr += it.entries.size();
		}

		coordinator = 0;
		threads.clear();
		new_positions.resize(phase1_buffers[table_index]->Count());
		for (uint32_t i = 0; i < num_threads; i++)
		{
			threads.push_back(thread(Phase3CThreadB, ref(coordinator), ref(buckets), ref(new_positions)));
		}

		for (auto &it: threads)
		{
			it.join();
		}

		cout << "Starting thread C" << endl;

		coordinator = 0;
		threads.clear();
		new_positions.resize(phase1_buffers[table_index]->Count());
		for (uint32_t i = 0; i < num_threads; i++)
		{
			threads.push_back(thread(Phase3CThreadC, ref(coordinator), ref(buckets), output_buffer, output_table_offset, table_index));
		}

		for (auto &it: threads)
		{
			it.join();
		}

		Encoding::ANSFree(kRValues[table_index - 1]);

		final_table_begin_pointers[table_index] = output_table_offset;
		// setup output_table_offset for next iteration
		uint64_t park_size_bytes = EntrySizes::CalculateParkSize(K, table_index);
		uint64_t parks_needed = (ctr+kEntriesPerPark-1)/kEntriesPerPark;
		output_table_offset = output_buffer->GetInsertionOffset(parks_needed*park_size_bytes);

		if (table_index == 1)
			delete phase1_buffers[0];
		if (table_index < 6)
			delete phase1_buffers[table_index];
	}
	final_table_begin_pointers[7] = output_table_offset;
	struct Phase3_Out o;
	o.output_buff = output_buffer;
	o.pointer_table_offset = pointers_offset;
	o.new_table7_positions = new_positions;
	return o;
}
