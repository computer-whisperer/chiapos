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
		if (bucket_id > buckets.size()){
			break;
		}
	    struct {
	        bool operator()(BucketEntry a, BucketEntry b) const { return a.line_point < b.line_point; }
	    } customLess;
		sort(buckets[bucket_id].entries.begin(), buckets[bucket_id].entries.end(), customLess);


		for (auto &it : buckets[bucket_id].entries)
		{
			new_positions[it.orig_pos] = (it - buckets[bucket_id].entries.begin()) + buckets[bucket_id].output_base_pos;
		}
	}
}

void Phase3CThreadC(atomic_long& coordinator, vector<Bucket>& buckets, Buffer& output_buffer, uint8_t table_index)
{
	while (1)
	{
		uint32_t bucket_id = coordinator.fetch_add(1);
		if (bucket_id > buckets.size()){
			break;
		}




	}
}


void Phase3C(vector<Buffer*> phase1_buffers, vector<vector<atomic<uint8_t>>> phase2_used_entries, uint32_t num_threads, string id, string memo, string dest_fname)
{
	phase1_buffers[0]->SwapInAsync(false);
	phase1_buffers[1]->SwapInAsync(false);

	Buffer output_buffer(100, dest_fname);

    // 19 bytes  - "Proof of Space Plot" (utf-8)
    // 32 bytes  - unique plot id
    // 1 byte    - k
    // 2 bytes   - format description length
    // x bytes   - format description
    // 2 bytes   - memo length
    // x bytes   - memo

    output_buffer.InsertString("Proof of Space Plot");
    output_buffer.InsertData((void*)id.data(), kIdLen);
    *(uint8_t*)(output_buffer.data + output_buffer.GetInsertionOffset(1)) = K;
    *(uint16_t*)(output_buffer.data + output_buffer.GetInsertionOffset(2)) = kFormatDescription.size();
    output_buffer.InsertString(kFormatDescription);
    *(uint16_t*)(output_buffer.data + output_buffer.GetInsertionOffset(2)) = memo.size();
    output_buffer.InsertString(memo);

    uint8_t pointers[10 * 8];
    memset(pointers, 0, 10 * 8);
    uint32_t pointers_offset = output_buffer.InsertData(pointers, sizeof(pointers));


    std::vector<uint64_t> final_table_begin_pointers(12, 0);
    final_table_begin_pointers[1] = output_buffer.insert_pos;


    // These variables are used in the WriteParkToFile method. They are preallocatted here
    // to save time.
    uint64_t park_buffer_size = EntrySizes::CalculateLinePointSize(k) +
                                EntrySizes::CalculateStubsSize(k) + 2 +
                                EntrySizes::CalculateMaxDeltasSize(k, 1);


	vector<uint32_t> new_positions;
	vector<Bucket> buckets(num_buckets);
	for (auto &it : buckets)
	{
		it.entries.reserve(mean_entries_in_bucket*1.2);
	}

	for (uint32_t table_index = 1; table_index < 6; table_index++)
	{
		if (table_index < 6)
		{
			for (auto &it : buckets)
			{
				it.entries.clear();
			}
		}

		if (table_index < 6)
		{
			phase1_buffers[table_index+1]->SwapInAsync(false);
		}

		phase1_buffers[table_index-1]->WaitForSwapIn();
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

		delete phase1_buffers[table_index-1];
	}
}
