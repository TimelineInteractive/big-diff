//
//  difflocator.h
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-27.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#ifndef Big_Diff_difflocator_h
#define Big_Diff_difflocator_h

class DiffLocator
{
    static const size_t buffer_size = 131072;
    int num_diffs_found = 0;
    boost::atomic<bool> diffs_done;
    std::unordered_set<Digest, Hash>* diff_set;
    std::string rdf_string;
    boost::lockfree::spsc_queue<std::string, boost::lockfree::capacity<1024> > diff_queue;
    std::ofstream out;
    boost::log::sources::severity_logger_mt< >& lg = my_logger::get();
    
    void ReadDiffs( std::string rdf_file_path );
    inline void Producer( void );
    inline void Consumer( void );
    inline bool LookupHash( std::unordered_set<Digest, Hash>& diffs, Digest& hash );
    void CheckString( std::string line );
    
public:
    DiffLocator( std::unordered_set<Digest, Hash>& all_diffs, std::string rdf_path, std::string final_diff_loc );
    void FindDiffs();
};

#endif
