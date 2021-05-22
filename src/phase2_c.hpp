#ifndef PHASE2_C_HPP
#include <vector>
#include <atomic>
#include "buffers.hpp"

std::vector<std::vector<std::atomic<uint8_t>>> Phase2C(std::vector<Buffer*> phase1_buffers, uint32_t num_threads);

#endif
