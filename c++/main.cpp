//
//  main.c
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-18.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#include "big_diff.h"

#include <fstream>
#include <iostream>
#include <boost/log/attributes/timer.hpp>
#include <boost/timer.hpp>
#include "my_logger.h"
#include <stdexcept>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <ctime>
#include <openssl/md5.h>
#include "bucketizer.h"
#include "differ.h"
#include <boost/thread.hpp>
#include <time.h>
#include "difflocator.h"
#include <sys/stat.h>
#include "test.h"

namespace keywords = boost::log::keywords;
namespace sinks = boost::log::sinks;
namespace attrs = boost::log::attributes;

using boost::shared_ptr;

// Declared in big_diff.h
std::string output_path;
std::string old_file_prefix = "old_";
std::string new_file_prefix = "new_";

void init_logger();
/*
 
 To Run Big Diff, Provide 3 Arguments:
    1) Path To Old Gzip Dump
    2) Path To New Gzip Dump
    3) Output Path

To Run Tests, Provide 1 Argument:
    - Path To Source Gzip Dump
 
 */
int main( int argc, const char * argv[] )
{
    time_t start = time(NULL);
    time_t timer;
    std::string old_gz_loc;
    std::string new_gz_loc;
    
    // Running Big Diff
    if ( argc == 4 )
    {
        old_gz_loc = argv[1];
        new_gz_loc = argv[2];
        std::string out_path = argv[3];
        if ('/' != out_path.back() )
        {
            out_path +='/';
        }
        output_path = out_path;
    }
    else if ( argc == 2 )
    {
        // Running Tests
        std::string test_file_loc = argv[1];
        std::cout << "Running Tests On File: "+std::string(test_file_loc)<< std::endl;
        
        Test* test = new Test( test_file_loc);
        test -> TestDiffAccuracy( 100000, 10000 );
        test -> TestHashDiffing();
        test -> TestHashSpeed( 50000000 );
        exit(0);
        
    }
    else
    {    // Insufficient Arguments
        printf("To Run Big Diff, Provide 3 arguments: <old-gzip-path>, <new-gzip-path> <output-path>\nTo Test, Provide 1 Argument <gzip-path>\n");
        exit(1);
    }

    // Initialize Paths
    std::string old_path_prefix = output_path + old_file_prefix;
    std::string new_path_prefix = output_path + new_file_prefix;
    std::string final_loc_added = output_path + "final_diffs_added.txt";
    std::string final_loc_deleted = output_path + "final_diffs_deleted.txt";

    // Initialize Logger
    boost::log::sources::severity_logger_mt< >& lg = my_logger::get();
    init_logger();
  
    boost::log::add_file_log( output_path + "hash.log" );
    lg.add_attribute( "Duration", boost::log::attributes::timer() );
  
    /*
     First Phase:
        For each source rdf, create a thread to read lines, hash them, and store the hashes in buckets corresponding to their hash prefix.
    */

    // Create Bucket Location
    BOOST_LOG(lg) << "Initializing bucket location";
    std::string output_dir ( output_path );
    int no_dir = mkdir( output_dir.c_str(), 0777 );
  
    // Bucketize First Rdf Gzip
    time_t bucket_start = time(NULL);
    Bucketizer *first = new Bucketizer( old_gz_loc, output_dir, old_file_prefix );
    boost::thread first_b_t(&Bucketizer::Bucketize, first);

    // Bucketize Second Rdf Gzip
    Bucketizer *second = new Bucketizer( new_gz_loc, output_dir, new_file_prefix );
    boost::thread second_b_t( &Bucketizer::Bucketize, second );

    first_b_t.join();
    delete first;

    double first_bucket = difftime( time(&timer), start );
    BOOST_LOG(lg) << "First Bucketize: " << first_bucket << " seconds";

    second_b_t.join();
    delete second;

    double bucket_elapsed = difftime( time(&timer), start );
    double second_bucket = difftime( time(&timer), bucket_start );
    BOOST_LOG(lg) << "Second Bucketize: " << second_bucket << " seconds";

    BOOST_LOG(lg) << "Finished Bucketize: " << bucket_elapsed << " seconds";

    /*
        Second Phase:
            Run diffs on each binary bucket, storing the diff hashes in an set for next-phase lookup
    */
    
    // Sort and Diff All New/Old Bucket Pairs
    std::unordered_set<Digest, Hash> added_diffs;
    std::unordered_set<Digest, Hash> deleted_diffs;
    added_diffs.reserve( lines_size );
    deleted_diffs.reserve( lines_size );

    time_t diff_start = time(NULL);
    Differ* differ = new Differ( added_diffs, deleted_diffs, output_path );
    differ -> DiffBuckets();
  
    double diff_elapsed = difftime( time(NULL), diff_start );
    BOOST_LOG(lg) << "Finished Sort/Diff Buckets Phase: "<< diff_elapsed << " seconds";
    std::cout << "Finished Sort/Diff Buckets Phase: "<< diff_elapsed << " seconds";
    delete differ;
    
    /*
     Third Phase:
            Create a thread for each source gzip and locate the lines corresponding to the stored diff hashes from the previous phase by reading through the source and hashing again. Write the located lines to the corresponding output file.
	*/

    // Find Diffs in Sorted Buckets ( One Call Per RDF Gzip Dump )
    time_t diffloc_start = time(NULL);
    
    DiffLocator* deleted = new DiffLocator( deleted_diffs, old_gz_loc, final_loc_deleted );
    boost::thread first_diff_loc( &DiffLocator::FindDiffs, deleted );
  
    DiffLocator* added = new DiffLocator( added_diffs, new_gz_loc, final_loc_added );
    boost::thread second_diff_loc( &DiffLocator::FindDiffs, added );

    first_diff_loc.join();
    double first_diff = difftime( time(NULL), diffloc_start );
    BOOST_LOG(lg) << "Deleted Diffs From "+ old_gz_loc+" Written in: " << first_diff <<" seconds";

    second_diff_loc.join();
    double second_diff = difftime( time(NULL), diffloc_start );
    BOOST_LOG(lg) << "Added Diffs From "+ new_gz_loc+" Written in: " << second_diff <<" seconds";

    double diffs_elapsed = difftime( time( NULL), diffloc_start );
    BOOST_LOG(lg) << "Finished Locating Diffs In Source in: "<< diffs_elapsed <<" seconds";

    double total_elapsed = difftime( time(NULL), start );
    BOOST_LOG(lg) << "Big Diff Finished in: "<< total_elapsed <<" seconds";

    return 0;
}

void init_logger()
{
    try
    {
        typedef sinks::synchronous_sink< sinks::text_file_backend > file_sink;
 
        boost::shared_ptr< file_sink > sink = boost::make_shared<file_sink>(
            keywords::rotation_size = 10 * 1024 * 1024,         // Log Rotation Size
            keywords::auto_flush = true,                        // Flush Each Log Record to File
            keywords::format = "[%TimeStamp%]: %Message%"       // Log Entry Timestamp
        );
  
        // Set up where the rotated files will be stored
        sink->locked_backend()->set_file_collector(sinks::file::make_collector(
            keywords::target = output_path+"log"             // where to store rotated files
        ));
  
        // Upon restart, scan the target directory for files matching the file_name pattern
        sink->locked_backend()->scan_for_files();
  
        // Add it to the core
        boost::log::core::get()->add_sink(sink);
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << std::endl;
    }
}



