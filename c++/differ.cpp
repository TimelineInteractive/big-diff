//
//  sort_and_diff.cpp
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-19.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#include "big_diff.h"
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <sys/stat.h>
#include <unordered_set>
#include <parallel/algorithm>
#include "my_logger.h"
#include "differ.h"

Differ::Differ( std::unordered_set<Digest, Hash>& added_diffs, std::unordered_set<Digest, Hash>& deleted_diffs, std::string output_path )
{
	// Initialize Comparison Vectors
	BOOST_LOG(lg) << "Initializing comparison vectors";
	old_data.reserve(lines_size);
	new_data.reserve(lines_size);

	output_filepath = output_path;

	// Initialize Pointers to Diff Sets
	added = &added_diffs;
	deleted = &deleted_diffs;
}

Differ::Differ(){;};

// Read All Lines From Given Bucket Into Vector
void Differ::FillVector( std::vector<Digest>& lines,  const std::string& filepath )
{
    std::ifstream in( filepath, std::ifstream::binary );
    unsigned char line[16];

    while(true)
    {
		in.read( reinterpret_cast<char*>( &line ), 16 );
		Digest current( line );
		if( in.eof() ) break;
		lines.push_back( current );
    }
    in.close();
}

// Order Vector by Value of Stored Digest
void Differ::SortVector( std::vector<Digest>& lines )
{
    __gnu_parallel::sort( lines.begin(), lines.end() );
}

// Compare Vectors 'new_data' and 'old_data' to Locate Diffs
void Differ::CompareVectors()
{
    int i = 0, j = 0, max_i = old_data.size(), max_j = new_data.size();
	while ( i<max_i || j <max_j ) {
		while ( i+1 < max_i && old_data[i] == ( old_data[i+1] ) ){
            i++;
		}
		while ( j+1 < max_j && new_data[j] == ( new_data[j+1] ) ){
            j++;
		}
		// Store Diffs in Respective Diff Set
		if ( i>= max_i && j< max_j ){
			added->insert( new_data[j++] );
		} else if ( j>= max_j && i< max_i ){
			deleted->insert( old_data[i++] );
		} else if ( old_data[i] == ( new_data[j] ) ){
            i++; j++;
		} else if ( old_data[i] < new_data[j] ){
            deleted->insert( old_data[i++] );
		} else if ( new_data[j] < old_data[i] ){
            added->insert( new_data[j++] );
		}
	}
}

/*
 For Each Pair of Buckets:
    1) Read Each Bucket From File Into Vectors
    2) Parallel Sort Each Vector
    3) Locate Diffs and Store to Set
*/
void Differ::DiffBuckets()
{
	int old_data_size = 0;
	int new_data_size = 0;
	time_t start, bucket_start, phase_start, current;
	double diff_t;

	// Load Vectors Sequentially From Same-Index Buckets
	time(&start);

    // Load From Each File To Vector and Sort
    for ( int i = 0; i <= num_files; i++ )
    {
		time(&bucket_start);
		std::stringstream sstream;
		sstream << std::hex << i;
		current_hex = sstream.str();
		BOOST_LOG( lg ) << "Comparing "+current_hex+" files";

		old_filepath = output_filepath + old_file_prefix + current_hex;
		new_filepath = output_filepath + new_file_prefix + current_hex;

		// Put Current Buckets in Vectors
		FillVector( old_data, old_filepath );
		FillVector( new_data, new_filepath );

		time(&current);
		diff_t = difftime( current, bucket_start );
		BOOST_LOG( lg )<< "bucket " + current_hex + " read in "<< diff_t << "s";

		// Sort Each Vector
		time( &phase_start );
		SortVector( old_data );
		SortVector( new_data );
		time( &current );
		diff_t = difftime( current, phase_start );
		BOOST_LOG(lg)<< "bucket " + current_hex + " sorted in "<< diff_t << "s";

		old_data_size += old_data.size();
		new_data_size += new_data.size();

		// Compare Contents and Update Diff Sets
		time(&phase_start);
		CompareVectors();
		time(&current);
		diff_t = difftime( current, phase_start );
		BOOST_LOG(lg)<< "Bucket " + current_hex + " compared in "<< diff_t << "s";

		// Clear the Vectors
		old_data.clear();
		new_data.clear();

		time(&current);
		diff_t = difftime( current, bucket_start );
		BOOST_LOG(lg)<< "Finished Diffing Bucket " + current_hex + " in "<< diff_t << "s";
		BOOST_LOG(lg) << "Generated Diffs From Files "+old_filepath +" and "+ new_filepath;
    }
}
