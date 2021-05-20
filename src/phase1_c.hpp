#ifndef PHASE1_C_HPP
#define PHASE1_C_HPP

#include <vector>
#include "buffers.hpp"

#define K 32

const uint32_t phase1_pos_size = K + 1;

struct Phase1Table1Entry
{
	uint64_t x : K;
};

struct Phase1PosOffsetEntry
{
	uint64_t pos : phase1_pos_size;
	uint64_t offset : kOffsetSize;
};

struct Phase1Table7Entry
{
	uint64_t y : K;
	uint64_t pos : phase1_pos_size;
	uint64_t offset : kOffsetSize;
};

std::vector<Buffer*> Phase1C(uint8_t const* id, uint32_t num_threads);

#endif
