#ifndef PHASE3_C_HPP
#include <vector>
#include <atomic>
#include "buffers.hpp"

void Phase3C(std::vector<Buffer*> phase1_buffers, std::vector<std::vector<std::atomic<uint8_t>>> phase2_used_entries, uint32_t num_threads);

#endif
