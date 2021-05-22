#include <cstdint>
#include <vector>
#include <thread>
#include <mutex>

#include "buffers.hpp"
#include "pos_constants.hpp"
#include "calculate_bucket.hpp"
#include "phase1_c.hpp"

using namespace std;

const uint64_t num_buckets = ((1ULL<<(K+kExtraBits))/kBC)+1;

const uint32_t bucketed_y_len = 14;
struct PosOffsetYCBucketedEntry
{
	uint64_t pos : phase1_pos_size;
	uint64_t offset : kOffsetSize;
	uint64_t y_offset : bucketed_y_len;
	uint128_t c;
};


struct Bucket
{
	uint64_t output_base_pos;
	vector<struct PosOffsetYCBucketedEntry> entries;
	mutex entriesMutex;
};


void assert_matching(int64_t lout, int64_t rout)
{

	int64_t bucket_id_lout = lout/kBC;
    int64_t bucket_id_rout = rout/kBC;
    assert(bucket_id_lout + 1 == bucket_id_rout);
    int64_t b_id_l = (lout%kBC)/kC;
    int64_t c_id_l = (lout%kBC)%kC;
    int64_t b_id_r = (rout%kBC)/kC;
    int64_t c_id_r = (rout%kBC)%kC;
    int64_t m = (kB + b_id_r - b_id_l)%kB;
    assert((uint64_t)m < (1UL<<kExtraBits));
    int64_t a = c_id_r - c_id_l;
    int64_t b = 2*m + (bucket_id_lout%2);
    int64_t c = (b*b)%kC;
    int64_t d = (kC + a)%kC;
    assert(c == d);

}

inline void insert_bits(uint8_t* dst, uint64_t dst_start_offset, uint8_t* src, uint64_t src_start_offset, uint64_t bit_num)
{
	uint64_t o = dst_start_offset/8;
	dst += o;
	dst_start_offset = dst_start_offset%8;

	o = src_start_offset/8;
	src += o;
	src_start_offset = src_start_offset%8;

	uint8_t temp_buff[(src_start_offset+bit_num+7)/8];

	// Iterate over all bytes we are allowed to read
	for (int i = 0; i < (src_start_offset+bit_num+7)/8; i++)
	{
		uint8_t read_byte = src[i];

		uint8_t next_part = (read_byte << src_start_offset);
		temp_buff[i] = next_part;

		if (i != 0)
		{
			uint8_t prev_part = (read_byte >> src_start_offset);
			temp_buff[i-1] |= prev_part;
		}
	}

	// Iterate over bytes we are allowed to write
	for (int i = 0; i < (dst_start_offset+bit_num+7)/8; i++)
	{
		uint8_t write_byte = 0;

		if (i == 0)
		{
			uint8_t start_mask = ~((1UL<<(8-dst_start_offset))-1);
			write_byte = dst[i]&start_mask;
		}
		else
		{
			write_byte = temp_buff[i-1]<<(8-dst_start_offset);
		}

		write_byte |= temp_buff[i]>>dst_start_offset;

		if (i+1 == (dst_start_offset+bit_num+7)/8)
		{
			uint8_t end_mask = ~(1UL<<(8-((dst_start_offset+bit_num)%8)));
			write_byte = (write_byte&~end_mask) | (dst[i]&end_mask);
		}

		dst[i] = write_byte;
	}
}

void* Phase1CThread(atomic_long& coordinator, uint8_t const* id, vector<Bucket>* read_buckets, vector<Bucket>* write_buckets, Buffer* output_buffer, int table_index)
{
	if (table_index == -1)
	{
		F1Calculator f1(K, id);

		while (1)
		{
			uint64_t batch_size = (1ULL<<kBatchSizes);
			uint64_t x = coordinator.fetch_add(batch_size);
			if (x >= (1ULL<<K))
				break;
			if ((x+batch_size) > (1ULL<<K))
			{
				batch_size = (1ULL<<K) - x;
			}
			uint64_t buff[batch_size];
			f1.CalculateBuckets(x, batch_size, buff);

			for (uint64_t i = 0; i < batch_size; i++)
			{
				struct PosOffsetYCBucketedEntry entry;
				entry.c = x+i;
				uint64_t bucket_id = buff[i]/kBC;
				entry.y_offset = buff[i]%kBC;
				write_buckets->at(bucket_id).entriesMutex.lock();
				write_buckets->at(bucket_id).entries.push_back(entry);
				write_buckets->at(bucket_id).entriesMutex.unlock();
			}
		}
	}
	else
	{
		vector<vector<uint16_t>> rmap(kBC);
	    std::vector<uint16_t> rmap_clean;
		FxCalculator fx(K, table_index+2);
		while (1)
		{
			uint64_t bucket_id = coordinator.fetch_add(1);
			if (bucket_id >= read_buckets->size())
				break;

	        uint16_t parity = bucket_id % 2;

	        if ((table_index < 6) && (bucket_id < read_buckets->size()-1))
	        {
		        for (size_t yl : rmap_clean) {
		            rmap[yl].clear();
		        }
		        rmap_clean.clear();
		        for (size_t pos_R = 0; pos_R < read_buckets->at(bucket_id+1).entries.size(); pos_R++) {
		            uint64_t r_y = read_buckets->at(bucket_id+1).entries[pos_R].y_offset;
		            if (rmap[r_y].size() == 0)
		            {
			            rmap_clean.push_back(r_y);
		            }
		            rmap[r_y].push_back(pos_R);
		        }
	        }

	        for (size_t pos_L = 0; pos_L < read_buckets->at(bucket_id).entries.size(); pos_L++) {
	            auto left_entry = read_buckets->at(bucket_id).entries[pos_L];
	            uint64_t left_y = left_entry.y_offset + bucket_id*kBC;
	            uint8_t * left_dest = output_buffer->data + (read_buckets->at(bucket_id).output_base_pos+pos_L)*output_buffer->entry_len;
	            if (table_index == 0)
	            {
	            	((struct Phase1Table1Entry *)left_dest)->x = left_entry.c;
	            }
	            else if (table_index < 6)
	            {
	            	((struct Phase1PosOffsetEntry *)left_dest)->pos = left_entry.pos;
	            	((struct Phase1PosOffsetEntry *)left_dest)->offset = left_entry.offset;
	            }
	            else
	            {
	            	((struct Phase1Table7Entry *)left_dest)->y = left_entry.y_offset + bucket_id*kBC;
	            	((struct Phase1Table7Entry *)left_dest)->pos = left_entry.pos;
	            	((struct Phase1Table7Entry *)left_dest)->offset = left_entry.offset;
	            }

	            if ((table_index < 6) && (bucket_id < (read_buckets->size()-1)))
	            {
		            for (uint8_t i = 0; i < kExtraBitsPow; i++) {
		                uint16_t r_target = L_targets[parity][left_entry.y_offset][i];
		                for (auto &pos_R : rmap[r_target])
		                {
		                	auto right_entry = read_buckets->at(bucket_id+1).entries[pos_R];
		                	uint64_t right_y = right_entry.y_offset + (bucket_id+1)*kBC;

		                	uint64_t c_len = kVectorLens[table_index+2] * K;

		                	auto out = fx.CalculateBucket(Bits(left_y, K+kExtraBits), Bits(left_entry.c, c_len), Bits(right_entry.c, c_len));

		                	struct PosOffsetYCBucketedEntry entry;
		                	uint64_t bucket_id_out = out.first.GetValue()/kBC;
		                	entry.pos = pos_L + read_buckets->at(bucket_id).output_base_pos;
		                	uint64_t offset = read_buckets->at(bucket_id+1).output_base_pos + pos_R - entry.pos;
		                	entry.offset = offset;
		                	//assert(offset < (1ULL<<kOffsetSize));
		                	entry.y_offset = out.first.GetValue()%kBC;
		                	if (table_index < 5)
		                	{
			                	uint8_t buff[16];
			                	memset(buff, 0, sizeof(buff));
			                	out.second.ToBytes(buff);
			                	entry.c = Util::SliceInt128FromBytes(buff, 0, kVectorLens[table_index+3] * K);
		                	}
		                	write_buckets->at(bucket_id_out).entriesMutex.lock();
		        			write_buckets->at(bucket_id_out).entries.push_back(entry);
		        			write_buckets->at(bucket_id_out).entriesMutex.unlock();
		                }
		            }
	            }
	        }
		}
	}
	return NULL;
}



vector<Buffer*> Phase1C(uint8_t const* id, uint32_t num_threads)
{
	cout << "Allocating buckets" << endl;
	vector<Bucket>* write_buckets = new vector<Bucket>(num_buckets);
	vector<Bucket>* read_buckets = new vector<Bucket>(num_buckets);
	for (auto &it : *write_buckets)
	{
		it.entries.reserve(300);
	}
	for (auto &it : *read_buckets)
	{
		it.entries.reserve(300);
	}

	vector<Buffer*> phase1_buffers;
	for (int table_index = -1; table_index < 7; table_index++)
	{
		vector<Bucket>* temp = read_buckets;
		read_buckets = write_buckets;
		write_buckets = temp;
		if (table_index < 6)
		{
			for (auto &it : *write_buckets)
			{
				it.entries.clear();
			}
		}

		Timer table_start_time;
		cout << "Starting table pass " << table_index << endl;


		Buffer* buffer;
		if (table_index == 0)
		{
			buffer = new Buffer(sizeof(struct Phase1Table1Entry)*(1ULL<<K));
			buffer->entry_len = sizeof(struct Phase1Table1Entry);
		}
		else if (table_index < 6)
		{
			buffer = new Buffer(sizeof(struct Phase1PosOffsetEntry)*(1ULL<<phase1_pos_size));
			buffer->entry_len = sizeof(struct Phase1PosOffsetEntry);
		}
		else if (table_index < 7)
		{
			buffer = new Buffer(sizeof(struct Phase1Table7Entry)*(1ULL<<phase1_pos_size));
			buffer->entry_len = sizeof(struct Phase1Table7Entry);
		}

		if (table_index >= 0)
		{
			// Fill output_base_pos
			uint64_t ctr = 0;
			for (auto &it: *read_buckets)
			{
				it.output_base_pos = ctr;
				ctr += it.entries.size();
			}
			*buffer->insert_pos = ctr*buffer->entry_len;
		}

		atomic_long coordinator = 0;

		cout << "Starting multithread section"<< endl;
		vector<thread> threads;
		for (uint32_t i = 0; i < num_threads; i++)
		{
			threads.push_back(thread(Phase1CThread, ref(coordinator), id, read_buckets, write_buckets, buffer, table_index));
		}

		for (auto &it: threads)
		{
			it.join();
		}
		cout << "Finished multithread section"<< endl;


		if (table_index >= 0)
		{
			if (table_index < 6)
				buffer->SwapOutAsync();
			phase1_buffers.push_back(buffer);
		}

		table_start_time.PrintElapsed("Table complete, time:");
	}
	delete read_buckets;
	delete write_buckets;
/*
	F1Calculator f1(K, id);
    for (uint64_t i = 0; i < phase1_buffers[1]->Count(); i++)
    {
    	struct Phase1PosOffsetEntry * entry = (struct Phase1PosOffsetEntry*)(phase1_buffers[1]->data + phase1_buffers[1]->entry_len*i);
		uint64_t pos = entry->pos;
		uint64_t offset = entry->offset;
		assert(offset > 0);
		assert(pos+offset < phase1_buffers[0]->Count());

		struct Phase1Table1Entry * lentry = (struct Phase1Table1Entry*)(phase1_buffers[0]->data + phase1_buffers[0]->entry_len*pos);
		struct Phase1Table1Entry * rentry = (struct Phase1Table1Entry*)(phase1_buffers[0]->data + phase1_buffers[0]->entry_len*(pos+offset));
		Bits fl = f1.CalculateF(Bits(lentry->x, K));
		Bits fr = f1.CalculateF(Bits(rentry->x, K));
		assert_matching(fl.GetValue(), fr.GetValue());
    }

	// Test that we can draw good proofs from this
	uint64_t challenge = 0x2c16;
	challenge = challenge%(1ULL<<K);
	uint64_t i;
	cout << "Looking for : " << challenge << endl;

	for (i = 0; i < phase1_buffers[6]->Count(); i++)
	{
		Phase1Table7Entry * entry = (Phase1Table7Entry *)(phase1_buffers[6]->data + phase1_buffers[6]->entry_len*i);
		if (entry->y == challenge)
		{
			break;
		}
	}
	if (i < phase1_buffers[6]->Count())
	{
		cout << "Got inputs: ";
		vector<uint64_t> x(64);
		for (uint32_t j = 0; j < 64; j++)
		{
			uint64_t next_pos = i;
			uint32_t l;
			for (l = 6; l > 0; l--)
			{

				uint64_t pos, offset;
				if (l == 6)
				{
					Phase1Table7Entry * entry = (Phase1Table7Entry *)(phase1_buffers[l]->data + phase1_buffers[l]->entry_len*next_pos);
					pos = entry->pos;
					offset = entry->offset;
				}
				else
				{
					Phase1PosOffsetEntry * entry = (Phase1PosOffsetEntry *)(phase1_buffers[l]->data + phase1_buffers[l]->entry_len*next_pos);
					pos = entry->pos;
					offset = entry->offset;
				}
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
				assert(next_pos < phase1_buffers[l-1]->Count());
			}
			Phase1Table1Entry * entry = (Phase1Table1Entry *)(phase1_buffers[l]->data + phase1_buffers[l]->entry_len*next_pos);
			x[j] = entry->x;
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
				F1Calculator f1(K, id);
				for (uint32_t i = 0; i < x.size(); i++)
				{
					Bits b = Bits(x[i], K);
					output_collations.push_back(b);
					output_fs.push_back(f1.CalculateF(b));
				}
			}
			else
			{
				FxCalculator fx(K, fi);
				for (uint32_t i = 0; i < input_collations.size(); i += 2)
				{
					assert_matching(input_fs[i].GetValue(), input_fs[i+1].GetValue());
					auto out = fx.CalculateBucket(input_fs[i], input_collations[i], input_collations[i+1]);
					output_fs.push_back(out.first);
					output_collations.push_back(out.second);
				}
			}
			input_collations = output_collations;
			input_fs = output_fs;
		}
		cout << "Result of tree: f7(...) = " << input_fs[0].GetValue()%(1ULL<<K) << endl;

	}
	else
	{
		cout << "Could not find " << challenge << " in table 7." << endl;
	}
*/

	return phase1_buffers;
}
