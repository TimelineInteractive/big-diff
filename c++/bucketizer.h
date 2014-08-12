#ifndef BUCKETIZER_H
#define BUCKETIZER_H

#include <boost/lockfree/spsc_queue.hpp>
#include <boost/atomic.hpp>
#include "my_logger.h"
#include "digest.h"
#include <vector>

class Bucketizer
{
	boost::lockfree::spsc_queue<Digest, boost::lockfree::capacity<1024> > spsc_queue;
	boost::log::sources::severity_logger_mt< >& lg = my_logger::get();
	boost::atomic<bool> done;

	int num_buckets = 0xff;
	int buffer_size = 262144;
	unsigned int index = 0;

	std::vector<FILE*> bucket_writers;

	std::string file_prefix;
	std::string in_file_path;
	std::string out_dest_path;

	std::vector<FILE*> InitializeWriters( const char* dest_path, std::string prefix );
	void CloseAllFiles();
	inline void Producer();
	void Consumer();
	inline void WriteHashToFile( Digest& value );

	public:
		Bucketizer( std::string gz_path, std::string out_path, std::string prefix );
		void Bucketize();
};

void WriteHashToFile( Digest& value, FILE* file );

#endif
