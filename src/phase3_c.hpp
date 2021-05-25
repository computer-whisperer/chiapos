#ifndef PHASE3_C_HPP
#define PHASE3_C_HPP
#include <vector>
#include <atomic>
#include <string>
#include "buffers.hpp"

struct Phase3_Out
{
	Buffer* output_buff;
	uint64_t pointer_table_offset;
	std::vector<uint32_t> new_table7_positions;
};

struct Phase3_Out Phase3C(
		std::vector<Buffer*> phase1_buffers,
		std::vector<std::vector<std::atomic<uint8_t>>>& phase2_used_entries,
		uint32_t num_threads,
		const uint8_t* memo,
        uint32_t memo_len,
        const uint8_t* id,
        uint32_t id_len,
		std::string dest_fname);

#endif
