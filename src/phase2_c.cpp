#include "phase2_c.hpp"

#include <atomic>
#include <thread>
#include <vector>

#include "buffers.hpp"
#include "phase1.hpp"
#include "util.hpp"

using namespace std;

const uint32_t block_size = 1ULL<<12;

void Phase2CThreadA(Buffer* phase1_buffer, vector<vector<atomic<uint8_t>>>& used_entries, atomic_long& coordinator, uint32_t table_index)
{

	while (1)
	{
		uint32_t block_id = coordinator.fetch_add(1);
		if (block_id*block_size > (phase1_buffer->Count())){
			break;
		}
		uint64_t num_in_block = block_size;
		if (num_in_block > (phase1_buffer->Count() - (block_id*block_size)))
		{
			num_in_block =  (phase1_buffer->Count() - (block_id*block_size));
		}

		for (uint64_t i = block_id*block_size; i < block_id*block_size + num_in_block; i++)
		{
			if ((table_index < 6) && ((used_entries[table_index][i/8] & (1U << (i%8))) == 0))
			{
				continue;
			}

			uint8_t* entry = phase1_buffer->data + i*phase1_buffer->entry_len;
			uint64_t pos;
			uint64_t offset;
			if (table_index == 6)
			{
				offset = ((struct Phase1Table7Entry*)entry)->offset;
				pos = ((struct Phase1Table7Entry*)entry)->pos;
			}
			else
			{
				offset = ((struct Phase1PosOffsetEntry*)entry)->offset;
				pos = ((struct Phase1PosOffsetEntry*)entry)->pos;
			}

			used_entries[table_index-1][pos/8].fetch_or(1U << (pos%8));
			used_entries[table_index-1][(pos+offset)/8].fetch_or(1U << ((pos+offset)%8));
		}
	}
}


vector<vector<atomic<uint8_t>>> Phase2C(vector<Buffer*> phase1_buffers, uint32_t num_threads)
{
	vector<vector<atomic<uint8_t>>> used_entries(6);
	for (uint32_t table_index = 6; table_index > 0; table_index--)
	{
		used_entries[table_index-1] = vector<atomic<uint8_t>>((phase1_buffers[table_index-1]->Count() + 7)/8);
		for (auto &it : used_entries[table_index-1])
		{
			it = 0;
		}

		if (table_index > 1)
		{
			phase1_buffers[table_index-1]->SwapInAsync(false);
		}

		phase1_buffers[table_index]->WaitForSwapIn();

		atomic_long coordinator = 0;
		vector<thread> threads;
		for (uint32_t i = 0; i < num_threads; i++)
		{
			threads.push_back(thread(Phase2CThreadA, phase1_buffers[table_index], ref(used_entries), ref(coordinator), table_index));
		}

		//phase1_buffers[table_index]->SwapOutAsync();

		for (auto &it: threads)
		{
			it.join();
		}
	}
	return used_entries;
}
