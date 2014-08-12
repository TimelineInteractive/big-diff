#ifndef DIFFER_H
#define DIFFER_H

#include <vector>
#include "digest.h"
#include "hash.h"
#include <unordered_set>

class Differ
{
	std::string current_hex;
	std::string old_filepath;
	std::string new_filepath;
	std::string output_filepath;
	std::vector<Digest> old_data;
	std::vector<Digest> new_data;
	boost::log::sources::severity_logger_mt< >& lg = my_logger::get();
	std::unordered_set<Digest, Hash>* added;
	std::unordered_set<Digest, Hash>* deleted;
	const static int num_files = 0xff;

	void SortVector( std::vector<Digest>& lines );
	void CompareVectors();

	public:
		Differ( std::unordered_set<Digest, Hash>& added_diffs, std::unordered_set<Digest, Hash>& deleted_diffs, std::string output_path );
		Differ();
		void DiffBuckets();
		void FillVector( std::vector<Digest>& lines,  const std::string& filepath );
};

#endif
