#include "buffers.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include <iostream>
#include <cerrno>

using namespace std;

std::string buffer_tmpdir = "/tmp";

Buffer::Buffer(const uint64_t size)
{
	fname = buffer_tmpdir + "/chiapos_tmp_buff_" + to_string(rand()) + ".buf";
    data_len = size;
    insert_pos = new std::atomic<uint64_t>(0);
    SwapIn();
}

uint64_t Buffer::GetInsertionOffset(uint64_t len)
{
  uint64_t offset = insert_pos->fetch_add(len);
  assert((offset+len) <= data_len);
  return offset;
}

uint64_t Buffer::PushEntry(Bits bits)
{
	uint64_t offset = GetInsertionOffset(entry_len);
	bits.ToBytes(data + offset);
	return offset;
}

uint64_t Buffer::Count()
{
	return *insert_pos/entry_len;
}

Buffer::~Buffer()
{
  SwapOut();
  remove(fname.c_str());
}

void Buffer::SwapOut()
{
    munmap(data, data_len);
    close(fd);
    data = NULL;
}

void Buffer::SwapOutAsync()
{
	swapoutthread = thread(&Buffer::SwapOut, this);
}

void Buffer::SwapIn()
{
	fd = open(fname.c_str(), O_RDWR|O_CREAT);
	int res = ftruncate(fd, data_len);
	if (res < 0)
	{
		std::cout << "log(-1) failed: " << std::strerror(errno) << '\n';
	}
    data = (uint8_t *) mmap(NULL, data_len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert(data != MAP_FAILED);
}

void Buffer::SwapInAsync()
{
	swapinthread = thread(&Buffer::SwapIn, this);
}
