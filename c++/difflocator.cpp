//
//  difflocator.cpp
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-19.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#include <unordered_set>
#import "big_diff.h"
#include <zlib.h>
#include <sstream>
#include <boost/thread.hpp>
#include "my_logger.h"
#include <boost/atomic.hpp>
#include <boost/thread.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <fstream>
#include "digest.h"
#include "hash.h"
#include "difflocator.h"

DiffLocator::DiffLocator( std::unordered_set<Digest, Hash>& all_diffs, std::string rdf_path, std::string final_diff_location )
{
    diffs_done = false;
    diff_set = &all_diffs;
    rdf_string = rdf_path;
    std::string fileStatus;
    
    out.open( final_diff_location );
    out.is_open()? fileStatus = " is": fileStatus = " is not";
    std::cout << "Difflocator ofstream to " << final_diff_location << fileStatus <<" open"<< std::endl;
}

/* Read from given gzip, for each line:
    1) Hash the line
    2) Perform a O(1) lookup of the hash in the diff set
    3) If the line hash is contained, write the line to the corresponding output diff file
*/
void DiffLocator::ReadDiffs( std::string rdf_file_path )
{
    gzFile inFile = gzopen( rdf_file_path.c_str(), "rb" );
    
    if ( !inFile )
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", rdf_file_path.c_str() );
        exit(-1);
    }
    time_t start = time( NULL );
    int did_hash;
    std::string current_line;
    double diff_t;
    unsigned long lineIndex = 0;
    std::cout << "Diff size before pulling diffs: " << diff_set->size() << std::endl;

    while ( true )
    {
        char line_buffer[buffer_size];
        char* line = gzgets( inFile, line_buffer, buffer_size );
    	if ( gzeof( inFile ) ) { break; }
        lineIndex++;
      
        if ( line != NULL )
        {
        	current_line = std::string( line );
            while ( !diff_queue.push( current_line ) ) {;}
        } else {
        	if ( !gzeof( inFile ) ) fprintf( stderr, "Unable to parse line %lu\n%s", lineIndex, line );
        }
        if ( lineIndex % 10000000 == 0 )
        {
            diff_t = difftime( time( NULL ), start );
            std::cout << lineIndex/1000000 << "m in " << diff_t << "s" << std::endl;
        }
    }
    std::cout << "Diff size: " << diff_set->size() << std::endl;
    gzclose( inFile );
}

bool DiffLocator::LookupHash( std::unordered_set<Digest, Hash>& diffs, Digest& hash )
{
    std::unordered_set<Digest, Hash>::const_iterator found = diffs.find( hash );     // Return Whether Set Contains Hash
    if ( found == diffs.end() )
    {
    	return false;
    }
    else {
    	diffs.erase( found );
    	num_diffs_found++;
    	return true;
    }
}

void DiffLocator::Producer( void )
{
  ReadDiffs( rdf_string );
}

void DiffLocator::Consumer( void )
{
  std::string value;
  while( !diffs_done ){
    while ( diff_queue.pop( value ) )
    {
      CheckString( value );
    }
  }
  
  while ( diff_queue.pop( value ) )
  {
    CheckString( value );
  }
}

void DiffLocator::CheckString( std::string line )
{
	unsigned char digest[16];  // Check If Current Line is a Stored Diff

	DigestLine( line.c_str(), digest );
	Digest hash = Digest( digest );
    
	if ( LookupHash( *diff_set, hash ) ) out << line;  // Save Line to Output Diff File
}

void DiffLocator::FindDiffs()
{
    boost::thread producer_thread( &DiffLocator::Producer, this );
    std::cout << "Number of Diffs Before Consumer Starts:" << diff_set->size() << std::endl;
    boost::thread consumer_thread( &DiffLocator::Consumer, this );
  
    producer_thread.join();
    diffs_done = true;
    BOOST_LOG(lg) << "DiffLocator Producer Has Finished";
    consumer_thread.join();
    BOOST_LOG(lg) << "DiffLocator Consumer Has Finished";

    std::cout << "Found Diffs This Run: " << num_diffs_found << std::endl;
    out.flush();
    out.close();
}
