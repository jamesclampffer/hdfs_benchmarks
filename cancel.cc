#include "hdfspp/hdfs_ext.h"


#include <iostream>
#include <sstream>
#include <string>
#include <exception>
#include <vector>
#include <mutex>
#include <thread>

#include "stdlib.h"
#include "string.h"

using namespace std;



/*



    hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                          int bufferSize, short replication, tSize blocksize);




*/

typedef std::lock_guard<std::mutex> mtx_guard;


struct file_open_error : public std::exception {
  std::string msg;
  file_open_error(const std::string &m) : msg(m) {}
  virtual const char *what() const throw() {
    return msg.c_str();
  };
};

struct file_read_error : public std::exception {
  std::string msg;
  file_read_error(const std::string &m) : msg(m) {}
  virtual const char *what() const throw() {
    return msg.c_str();
  };
};



struct range {
  size_t min;
  size_t max;

  range() : min(0), max(0) {}
  range(size_t max) : min(0), max(max) {}
  range(size_t min, size_t max) : min(min), max(max) {}
};


struct seek_read_batch {
  //todo: check all return codes, first make sure valgrind/asan is happy

  int id;
  hdfsFS file_system;
  std::vector<hdfsFile> files;
  int entry_size;
  range offset_minmax;
  std::mutex mtx;

  seek_read_batch(int id, hdfsFS fs, const std::vector<std::string> &sources, const range &seekrange) :
                  id(id), file_system(fs), entry_size(128), offset_minmax(seekrange)
  {
    for(unsigned int i=0;i<sources.size();i++) {
      hdfsFile f = hdfsOpenFile(file_system, sources[i].c_str(), 0, 0, 0,0);
      if(!f)
        throw file_open_error("unable to open" + sources[i]);
      files.push_back(f);
    }
  }

  void cancel_all()
  {
    mtx_guard guard(mtx);
    for(unsigned int i=0;i<files.size(); i++)
      hdfsCancel(file_system, files[i]);
  }

  void close_all()
  {
    mtx_guard guard(mtx);
    for(unsigned int i=0;i<files.size(); i++)
      hdfsCloseFile(file_system, files[i]);
  }

  size_t bytes_per_source;

  void do_read(hdfsFile file) {
    size_t read_count = 0;

    size_t min_record = offset_minmax.min / 128;
    size_t max_record = offset_minmax.max / 128;

    size_t range = max_record - min_record;


    while(read_count < bytes_per_source) {
      char *record[128];
      size_t record_offset = min_record + (double)rand() / RAND_MAX * range;
      size_t bytes_offset = record_offset * 128;

      int res = hdfsPread(file_system, file, bytes_offset, record, 128);
      if(res < 0)
        throw file_read_error("");
      //cout << (const char*)record << endl;
    }

  }
  
  void run(size_t bytes_per_source) {
    std::vector<std::thread> threads;
    this->bytes_per_source = bytes_per_source;
    for(unsigned int i=0; i<files.size(); i++) {
      hdfsFile file = files[i];
      threads.push_back(std::thread([this, file](){
        do_read(file);        
      }));
    }

    for(unsigned int i=0;i<files.size();i++) {
      threads[i].join();
    }
 
  }
};


const char * hdfsfiles[16] = {
"/source_001.txt",
"/source_002.txt",
"/source_003.txt",
"/source_004.txt",
"/source_005.txt",
"/source_006.txt",
"/source_007.txt",
"/source_008.txt",
"/source_009.txt",
"/source_010.txt",
"/source_011.txt",
"/source_012.txt",
"/source_013.txt",
"/source_014.txt",
"/source_015.txt",
"/source_016.txt",
};





int main() {
  srand(31);
  std::vector<std::string> srcs;
  for(int i=0;i<16;i++) {
    srcs.push_back(hdfsfiles[i]);
  }
  
  std::cout << "connecting" << std::endl;
  hdfsFS fs = hdfsConnect("localhost", 9433);
  if(!fs) {
    std::cerr << "failed to connect" << std::endl;
  }

  // start firing off read batches
  seek_read_batch srb(0, fs, srcs, range(0, 1024*1024));
  srb.run(11024*128);

}




