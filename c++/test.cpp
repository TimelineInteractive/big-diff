#include <zlib.h>
#include "big_diff.h"
#include <stdlib.h>
#include <fstream>
#include <unordered_set>
#include "bucketizer.h"
#include "differ.h"
#include "difflocator.h"
#include <string>
#include <sys/stat.h>
#include "test.h"
#include <iostream>

Test::Test(std::string in_path)
{
    in_file_path = in_path;
}

/* 
    1) Read total_num_lines lines from a gzip, removing duplicates
    2) Write total_num_lines and (total_num_lines - num_diff_lines ) Lines from source into two separate gzip files.
    3) Big-Diff the two resulting gzips, expecting to locate num_diff_lines diffs
 */
void Test::TestDiffAccuracy( int total_num_lines, int num_diff_lines )
{
    // Required Test Collections
    std::unordered_set<std::string> duplicate_remover;
    std::unordered_set<Digest, Hash> added_diffs_test;
    std::unordered_set<Digest, Hash> deleted_diffs_test;
    std::unordered_set<std::string> expected_diffs;
    
	// Final Diff File Locations
    std::string final_del_loc = "/tmp/final_diff_deleted";
    std::string final_added_loc = "/tmp/final_diff_added";

	// Reserve Vector for Expected Lines
	expected_diffs.reserve(num_diff_lines);

	// Create Bucket Directory
	int no_dir = mkdir(test_output_dir.c_str(), 0777);

    // Open Two Output Files
    std::ofstream* small = new std::ofstream(small_file);
    
    std::ofstream* large = new std::ofstream(large_file);

    duplicate_remover.reserve(120000);
    if (!small)
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", small_file.c_str() );
        exit(-1);
    }
    if (!large)
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", large_file.c_str() );
        exit(-1);
    }
    // Read Line From Gzip Into Queue
    gzFile in_file = gzopen(in_file_path.c_str(), "rb");

    if (!in_file)
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", in_file_path.c_str() );
        exit(-1);
    }

    unsigned long lineIndex = 0;
    char* line;


    while(true)
    {
    	char lineBuffer[buffer_size];
		line = gzgets(in_file, lineBuffer, buffer_size);

		if (duplicate_remover.size() == total_num_lines)
		{
			break;
		}

		lineIndex++;

		int line_valid = 0;

		if (line != NULL)
		{
			duplicate_remover.insert(std::string(line));
			line_valid = 1;
		}

		if (!line_valid && !gzeof(in_file))
		{
			fprintf(stderr, "Unable to parse line %lu\n%s", lineIndex, line);
			int* error;
			std::string linebuff (lineBuffer);
			std::cout << "Buffer: " + linebuff;
			std::cout << gzerror(in_file, error) << error;
		}
		if (lineIndex % 10000 == 0 ){
			std::cout << ".";
		}
    }
    std::cout <<"SIZE OF DUPLICATE REMOVER: "<< duplicate_remover.size()<<std::endl;
    gzclose(in_file);
    
    // Write To Files From Set (Duplicates Are Removed)
   
    int split_number = total_num_lines - num_diff_lines;
    int visited = 0;
    
    for( auto currentLine: duplicate_remover ){
        if(visited < split_number){
            *small << currentLine;
            *large << currentLine;
        } else {
            *large << currentLine;
            expected_diffs.insert(currentLine);
        }
        visited ++;
    }
    duplicate_remover.clear();

    std::cout <<"SIZE OF EXPECTED DIFF SET: "<< expected_diffs.size()<<std::endl;

    small->flush();
    small->close();
    large->flush();
    large->close();

    // Gzip Two Files
    GzipFile(small_file);
    GzipFile(large_file);
    
    // Bucketize Two Files
    Bucketizer* old_bucketizer = new Bucketizer(small_file+".gz", test_output_dir, old_file_prefix);
    old_bucketizer->Bucketize();
    
    Bucketizer* new_bucketizer = new Bucketizer(large_file+".gz", test_output_dir, new_file_prefix);
    new_bucketizer->Bucketize();

    // Diff The Two Sets
    added_diffs_test.reserve(120000);
    deleted_diffs_test.reserve(120000);
    Differ* differ = new Differ(added_diffs_test,deleted_diffs_test, test_output_dir);

    std::cout << "Before Diff Buckets: Deleted Lines : "+std::to_string(deleted_diffs_test.size()) << " Added Lines: " +std::to_string(added_diffs_test.size()) << std::endl;

    differ->DiffBuckets();
    
    std::cout << "After Diff Buckets: Deleted Lines : "+std::to_string(deleted_diffs_test.size()) << " Added Lines: " +std::to_string(added_diffs_test.size()) << std::endl;

    DiffLocator* deleted = new DiffLocator(deleted_diffs_test, small_file+".gz",final_del_loc);
    DiffLocator* added = new DiffLocator(added_diffs_test, large_file+".gz",final_added_loc);

    deleted->FindDiffs();
    added->FindDiffs();
    
    int added_size = Test::CountLines(final_added_loc);
    int del_size = Test::CountLines(final_del_loc);
    int small_txt_lines = Test::CountLines(small_file);
    int large_txt_lines = Test::CountLines(large_file);

    std::cout << "Small Txt File Has "<< small_txt_lines<< " Lines, Large Text File Has: "<<large_txt_lines<<" Lines"<<std::endl;

    int total_lines = added_size+del_size;
    
    std::cout << "Deleted Lines : "+std::to_string(del_size) << " Added Lines: " +std::to_string(added_size) << std::endl;

    // Compare Expected Diff Lines With Actual Diff Lines
    std::string temp;
    std::ifstream added_in(final_added_loc);
    std::ifstream deleted_in(final_del_loc);

    if (added_size)
    {
    	std::cout<< "Comparing Added Diffs Against Expected: "<<std::endl;
    	LookupDiffs(added_in, expected_diffs);
    } else { std:: cout << " There Were No Added Diffs" << std::endl; };

    if (del_size) {
    	std::cout<< "Comparing Deleted Diffs Against Expected: "<<std::endl;
    	LookupDiffs(deleted_in,expected_diffs);
    } else { std:: cout << "There Were No Deleted Diffs" << std::endl; };

    if ( expected_diffs.size() == total_lines ) {
    	std::cout << "SUCCESS: All " << total_lines << " Diff Lines Located in Expected Diff Set"<<std::endl;
    } else {
        std::cout << "FAIL: Expecting " + std::to_string(expected_diffs.size())+ " diff lines, found: "+std::to_string(total_lines)<<std::endl;
    }
}

void Test::GzipFile(std::string file_to_zip){
    // Use System Call to Gzip
    std::string gzipFile = "gzip -c "+ file_to_zip + " > " + file_to_zip+".gz";
    system(gzipFile.c_str());
}

int Test::CountLines(std::string filepath)
{
	std::ifstream in(filepath);
	std::string temp;
	int count = 0;
	while(true) {
		std::getline(in, temp);
		if( in.eof() ) break;
		count++;
	}
	return count;
}

/*
    Return whether all lines from input are contained in expected
 */
void Test::LookupDiffs(std::ifstream& in, std::unordered_set<std::string>& expected)
{
	std::string temp;
	int found = 0;
    
	while(true) {
		std::getline(in, temp);
		if( in.eof()) break;
		std::unordered_set<std::string>::const_iterator foundLine = expected.find(temp);
		if ( foundLine == expected.end() )
		{
			found = 0;
		}
	}
    
	if (found){
		std::cout << "All Diffs Found in Expected Diffs" << std::endl;
	};
}


/*
    1) Read 10 Lines From Source Gzip
    2) Hash the lines and store the hashes in memory
    3) Write the same hashes to file
    4) Read back and compare
 */
void Test::TestHashDiffing(){

	std::cout << "Starting Hash Read/Write Test"<<std::endl;

	std::string TEMP_FILE = "/tmp/hashtest";
	int NUM_LINES = 10;
	std::vector<std::string> lines;
	std::vector<Digest> lines_hashes;
	std::vector<Digest> read_hashes;

	// Reserve Memory for Vector Data
	lines.reserve(NUM_LINES);
	lines_hashes.reserve(NUM_LINES);
	read_hashes.reserve(NUM_LINES);

	unsigned char temp_digest[16];

	// Take the first NUM_LINES lines of a gzip, keep original lines and hashes in memory, write/read hashes to/from file to compare
	FILE* hashfile = fopen( TEMP_FILE.c_str(), "wb" );

    if ( !hashfile )
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", TEMP_FILE.c_str() );
        exit(-1);
    }

    gzFile inFile = gzopen( in_file_path.c_str(), "rb" );
    std::cout << "Opening Gzipped File: " + in_file_path << std::endl;

    if (!inFile)
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", in_file_path.c_str() );
        exit(-1);
    }
    unsigned long line_index = 0;
    char* line;
    int valid_line = 0;
    Digest current_digest;
    int write_count = 0;

    do
    {
        char line_buffer[buffer_size];
        line = gzgets( inFile, line_buffer, buffer_size );
        line_index++;
        valid_line = 0;

        if (line != NULL)
        {
        	std::cout << line_index << ": " << line <<std::endl;

        	// Store Line in Vector
        	std::string current_line(line);
            lines.push_back(current_line);
            DigestLine(line, temp_digest);

            // Store Line Digest in Vector
            current_digest = Digest(temp_digest);
            lines_hashes.push_back(current_digest);

            // Write Line Digest To File
            WriteHashToFile( current_digest, hashfile );
            write_count ++;
            valid_line = 1;
        }

        if ( !valid_line && !gzeof(inFile) )
        {
            fprintf( stderr, "Unable to parse line %lu\n%s", line_index, line );
            int* error;
            std::string linebuff (line_buffer);
            std::cout << "Buffer: " + linebuff;
            std::cout << gzerror( inFile, error ) << error;
        }
    }
    while (!gzeof(inFile) && line_index < 10);

    std::cout << "Wrote to file "<<std::to_string(write_count)<< " times"<<std::endl;

    gzclose(inFile);
    std::cout << std::to_string(lines_hashes.size()) << " Stored Hashes before Differ Fills Vector" << std::endl;

    // Close Hash File
    fflush( hashfile );
    fclose( hashfile );

    // Fill Vector With Digests From File
    Differ* differ = new Differ();
    differ->FillVector( read_hashes, TEMP_FILE );

    std::cout << std::to_string(read_hashes.size()) << " Read Hashes" << std::endl;
    std::cout << std::to_string(lines_hashes.size()) << " Stored Hashes" << std::endl;
    std::cout << std::to_string(lines.size()) << " Lines From Gzip" << std::endl;

    for ( auto current : read_hashes )
    {
    	std::cout << "Hash From File: " << current.ConvertDigest() <<std::endl;
    }

    for ( auto current : lines_hashes )
    {
    	std::cout << "Hash From Source: " << current.ConvertDigest() <<std::endl;
    }
}

// Evaluate Hash Performance Between MD5 and MurmurHash3 By Reading 1 Million Lines and Hashing
void Test::TestHashSpeed(int num_lines){
    
    std::cout << "Starting Hash Speed Test"<<std::endl;
    std::unordered_set<std::string> lines;
    std::unordered_set<Digest, Hash> md5_hashes;
    std::unordered_set<Digest, Hash> murmur_hashes;
    std::unordered_set<Digest, Hash> md5_collisions;
    std::unordered_set<Digest, Hash> murmur_collisions;
    std::unordered_set<std::string> matched_lines;
    
	// Reserve Memory for Vector Data
    lines.reserve(num_lines);
    md5_hashes.reserve(num_lines);
    murmur_hashes.reserve(num_lines);
    md5_collisions.reserve(1000);
    murmur_collisions.reserve(1000);
    matched_lines.reserve(1000);
    
    gzFile inFile = gzopen( in_file_path.c_str(), "rb" );
    std::cout << "Opening Gzipped File: " + in_file_path << std::endl;
    
    if (!inFile)
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", in_file_path.c_str() );
        exit(-1);
    }
    unsigned long line_index = 0;
    char* line;
    int valid_line = 0;
    Digest current_digest;
    
    do
    {
        char line_buffer[buffer_size];
        line = gzgets( inFile, line_buffer, buffer_size );
        line_index++;
        valid_line = 0;
        
        if (line != NULL)
        {
        	// Store Lines in Set to Remove Duplicates
        	std::string current_line(line);
        	lines.insert(current_line);
            
            valid_line = 1;
        }
        
        if ( !valid_line && !gzeof(inFile) )
        {
            fprintf( stderr, "Unable to parse line %lu\n%s", line_index, line );
            int* error;
            std::string linebuff (line_buffer);
            std::cout << "Buffer: " + linebuff;
            std::cout << gzerror( inFile, error ) << error;
        }
    }
    while ( !gzeof(inFile) && line_index < num_lines );
    
    gzclose(inFile);
    
    unsigned char temp_digest[16];
    Digest digest;
    int md5_collision_count = 0;
    int murmur_collision_count = 0;
    
    // Hash All Lines with MD5
    time_t md5_start = time(NULL);
    for ( auto current: lines )
    {
        DigestLineMD5(current.c_str(), temp_digest);
        current_digest = Digest(temp_digest);
        std::unordered_set<Digest, Hash>::const_iterator found = md5_hashes.find(current_digest);
        if (found == md5_hashes.end()){
            md5_hashes.insert(current_digest);
        } else {
            md5_collision_count++;
        }
    }
    std::cout << "Hashing " << num_lines << " Lines with MD5: " << difftime(time(NULL), md5_start) << " sec" << std::endl;
    std::cout << "Found " << md5_collision_count << " Collisions" << std::endl;
    
    // Hash All Lines with Murmur3
    time_t murmur_start = time(NULL);
    for ( auto current: lines )
    {
        DigestLine(current.c_str(), temp_digest);
        current_digest = Digest(temp_digest);
        std::unordered_set<Digest, Hash>::const_iterator found = murmur_hashes.find(current_digest);
        if (found == murmur_hashes.end()){
            murmur_hashes.insert(current_digest);
        } else {
            murmur_collision_count++;
        }
    }
    std::cout << "Hashing " << num_lines << " Lines with Murmur3: " << difftime(time(NULL), murmur_start) << " sec" << std::endl;
    std:: cout << "Found " << murmur_collision_count << " Collisions" << std::endl;
    
    // Re-Read Source To Locate Collisions/Duplicates (Murmur Only)
    inFile = gzopen( in_file_path.c_str(), "rb" );
    
    if (!inFile)
    {
        fprintf( stderr, "Couldn't open %s for reading.\n", in_file_path.c_str() );
        exit(-1);
    }
    line_index = 0;
    valid_line = 0;
    
    do
    {
        char line_buffer[buffer_size];
        line = gzgets( inFile, line_buffer, buffer_size );
        line_index++;
        valid_line = 0;
        
        if (line != NULL)
        {
        	// Store Line Hashes in Vector
        	std::string current_line(line);
            DigestLine(line, temp_digest);
            current_digest = Digest(temp_digest);
            
            std::unordered_set<Digest, Hash>::const_iterator found = murmur_collisions.find (current_digest);
            if (found != murmur_collisions.end())
            {
                matched_lines.insert(current_line);
            }
            
            valid_line = 1;
        }
        
        if ( !valid_line && !gzeof(inFile) )
        {
            fprintf( stderr, "Unable to parse line %lu\n%s", line_index, line );
            int* error;
            std::string linebuff (line_buffer);
            std::cout << "Buffer: " + linebuff;
            std::cout << gzerror( inFile, error ) << error;
        }
    }
    while ( !gzeof(inFile) && line_index < num_lines );
    
    gzclose(inFile);

    std::cout << "Murmur Collisions: "<< std::endl;
    if ( matched_lines.size() ){
		for ( auto current: matched_lines )
		{
			std::cout << current << std::endl;
		}
    } else {
    	std::cout << "None";
    }
}

