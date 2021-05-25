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

#ifndef SRC_CPP_PLOTTER_DISK_HPP_
#define SRC_CPP_PLOTTER_DISK_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <memory>

#include "chia_filesystem.hpp"

#include "calculate_bucket.hpp"
#include "encoding.hpp"
#include "exceptions.hpp"
#include "phase1_c.hpp"
//#include "phase2.hpp"
#include "phase2_c.hpp"
//#include "phase3.hpp"
#include "phase3_c.hpp"
#include "phase4_c.hpp"
#include "pos_constants.hpp"
#include "sort_manager.hpp"
#include "util.hpp"
#include "disk.hpp"

#define B17PHASE23

class DiskPlotter {
public:
    // This method creates a plot on disk with the filename. Many temporary files
    // (filename + ".table1.tmp", filename + ".p2.t3.sort_bucket_4.tmp", etc.) are created
    // and their total size will be larger than the final plot file. Temp files are deleted at the
    // end of the process.
    void CreatePlotDisk(
        std::string tmp_dirname,
        std::string final_dirname,
        std::string filename,
        const uint8_t* memo,
        uint32_t memo_len,
        const uint8_t* id,
        uint32_t id_len,
        uint32_t num_threads_input = 0,
        bool show_progress = false)
    {
    	buffer_tmpdir = tmp_dirname;

        // Increases the open file limit, we will open a lot of files.
#ifndef _WIN32
        struct rlimit the_limit = {600, 600};
        if (-1 == setrlimit(RLIMIT_NOFILE, &the_limit)) {
            std::cout << "setrlimit failed" << std::endl;
        }
#endif

        uint8_t num_threads;

        if (num_threads_input != 0) {
            num_threads = num_threads_input;
        } else {
            num_threads = 2;
        }

        std::cout << std::endl
                  << "Starting plotting progress into temporary dir: " << tmp_dirname << std::endl;
        std::cout << "ID: " << Util::HexStr(id, id_len) << std::endl;
        std::cout << "Plot size is: " << static_cast<int>(K) << std::endl;

        // Cross platform way to concatenate paths, gulrak library.
        std::vector<fs::path> tmp_1_filenames = std::vector<fs::path>();

        // The table0 file will be used for sort on disk spare. tables 1-7 are stored in their own
        // file.
        fs::path final_filename = fs::path(final_dirname) / fs::path(filename);

        // Check if the paths exist
        if (!fs::exists(tmp_dirname)) {
            throw InvalidValueException("Temp directory " + tmp_dirname + " does not exist");
        }

        if (!fs::exists(final_dirname)) {
            throw InvalidValueException("Final directory " + final_dirname + " does not exist");
        }
        for (fs::path& p : tmp_1_filenames) {
            fs::remove(p);
        }
        fs::remove(final_filename);

        std::ios_base::sync_with_stdio(false);
        std::ostream* prevstr = std::cin.tie(NULL);

		// Scope for FileDisk
		assert(id_len == kIdLen);

		std::cout << std::endl
				  << "Starting phase 1/4: Forward Propagation into tmp files... "
				  << TimerGetNow();

		Timer p1;
		Timer all_phases;
		std::vector<Buffer*> phase1_tables = Phase1C(id, num_threads);
		p1.PrintElapsed("Time for phase 1 =");

		uint64_t finalsize=0;


		// Memory to be used for sorting and buffers
		//std::unique_ptr<uint8_t[]> memory(new uint8_t[memory_size + 7]);

		std::cout << std::endl
			  << "Starting phase 2/4: Backpropagation ... "
			  << TimerGetNow();

		Timer p2;

		std::vector<std::vector<std::atomic<uint8_t>>> used_entries = Phase2C(phase1_tables, num_threads);
		p2.PrintElapsed("Time for phase 2 =");

		std::cout << "Starting phase 3/4: Compression from tmp files." << " ... " << TimerGetNow() << std::endl;
		Timer p3;
		struct Phase3_Out phase3_out = Phase3C(phase1_tables, std::ref(used_entries), num_threads, id, id_len, memo, memo_len, (std::string)final_filename);
		p3.PrintElapsed("Time for phase 3 =");

		std::cout << std::endl
			  << "Starting phase 4/4: Write Checkpoint tables" << " ... " << TimerGetNow();
		Timer p4;
		finalsize = Phase4C(phase1_tables[6], phase3_out.output_buff, phase3_out.pointer_table_offset, phase3_out.new_table7_positions, num_threads);
        //b17RunPhase4(k, k + 1, tmp2_disk, res, show_progress, 16);
        p4.PrintElapsed("Time for phase 4 =");

        phase3_out.output_buff->Truncate(finalsize);


        std::cin.tie(prevstr);
        std::ios_base::sync_with_stdio(true);

    }
};

#endif  // SRC_CPP_PLOTTER_DISK_HPP_
