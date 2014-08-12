//
//  bucketizer.cpp
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-20.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#include <boost/thread.hpp>
#include <iostream>
#include "big_diff.h"
#include <sstream>
#include <fstream>
#include <ctime>
#include "bucketizer.h"
#include <zlib.h>
#include <boost/date_time/posix_time/posix_time.hpp>

Bucketizer::Bucketizer( std::string gz_path, std::string out_path, std::string prefix )
{
	time_t start, current;
	double diff_t;

	// Initialize Class Members
	file_prefix = prefix;
	in_file_path = gz_path;
	out_dest_path = out_path;
	done = false;

	// Initialize Gzip and Output Paths
	BOOST_LOG(lg) << "Bucketizing From: " + in_file_path + " To: " + out_dest_path;

    // Initialize Writers
    BOOST_LOG(lg) << "Initializing Ofstream Bucket Writers";
    time( &start );
    bucket_writers = InitializeWriters( out_dest_path.c_str(), file_prefix );
    time( &current );
    diff_t = difftime( current,start );
    BOOST_LOG(lg) << "Time taken to initialize writers: " << diff_t << std::endl;
}

std::vector<FILE*> Bucketizer::InitializeWriters( const char* dest_path, std::string prefix )
{
    std::vector<FILE*> v;
    v.reserve( num_buckets + 1 );
    for ( int i = 0; i <= num_buckets; i++ )
    {
        std::stringstream stream;
        stream << std::hex << i;
        std::string hexID( stream.str() );
        std::string file_path = std::string( dest_path ) + prefix + hexID;
        BOOST_LOG(lg) << file_path;
        FILE* current_file = fopen( file_path.c_str(), "wb" );
        v.push_back( current_file );
    }
    return v;
}

void Bucketizer::CloseAllFiles()
{
    for ( int i = 0; i < bucket_writers.size(); i++ )
    {
    	fflush( bucket_writers[i] );
    	fclose( bucket_writers[i] );
    }
    bucket_writers.clear();
}

void Bucketizer::Consumer()
{
	Digest value;
	while( !done )
	{
		while ( spsc_queue.pop( value ) )
		{
			WriteHashToFile( value );
		}
	}

	while ( spsc_queue.pop( value ) )
	{
		WriteHashToFile( value );
	}
}

void Bucketizer::WriteHashToFile( Digest& value )
{
	index = value.raw_digest[0];
	FILE* file =  bucket_writers[index];
	fwrite( value.raw_digest, sizeof( unsigned char ), 16, file );
}

// For Testing Purposes
void WriteHashToFile( Digest& value, FILE* file )
{
	fwrite( value.raw_digest, sizeof( unsigned char ), 16, file );
}

void Bucketizer::Producer()
{
    // Read Line From Gzip Into Queue
    gzFile in_file = gzopen( in_file_path.c_str(), "rb" );
    time_t start, current, hash_start, hash_current;
    double diff_t, hash_diff_t;
    
    if ( !in_file )
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", in_file_path.c_str() );
        exit(-1);
    }

    time ( &start );
    unsigned long lineIndex = 0;
    char* line;
    hash_diff_t = 0;
    
    while( true )
    {
        char line_buffer[buffer_size];
        line = gzgets( in_file, line_buffer, buffer_size );
    	if ( gzeof( in_file ) ) { break; }
        lineIndex++;

        int did_hash = 0;

        if ( line != NULL )
        {
        	time( &hash_start );
        	unsigned char digest[16];
            DigestLine( line, digest );

            time( &hash_current );
            hash_diff_t = hash_diff_t + difftime( hash_current, hash_start );

            // Wait if Queue is Full
            while ( !spsc_queue.push( Digest( digest ) ) ) {;}

            did_hash = 1;
        }

        if ( !did_hash && !gzeof( in_file ) )
        {
            fprintf( stderr, "Unable to bucketize line %lu\n%s", lineIndex, line );
            int* error;
            std::string linebuff (line_buffer);
            std::cout << "Buffer: " + linebuff;
            std::cout << gzerror( in_file, error ) << error;
        }
        boost::log::sources::severity_logger_mt< >& lg = my_logger::get();
        if ( lineIndex % 10000000 == 0 )
        {
            time( &current );
            diff_t = difftime( current, start );
            std::cout << lineIndex/1000000 << "m in " << diff_t << "s using " << hash_diff_t << " for hashing" << std::endl;
            BOOST_LOG(lg) << lineIndex/1000000 << "m in " << diff_t << "s using " << hash_diff_t << " for hashing" << std::endl;
        }
    }
    gzclose( in_file );
}

void Bucketizer::Bucketize()
{
    // Initialize Threads
    done = false;
    boost::thread producer_thread( &Bucketizer::Producer, this );
    boost::thread consumer_thread( &Bucketizer::Consumer, this );
    
    producer_thread.join();
    done = true;
    BOOST_LOG(lg) << "Producer Has Finished";
    consumer_thread.join();
    
    BOOST_LOG(lg) << "Closing All Files";

    CloseAllFiles();
}
