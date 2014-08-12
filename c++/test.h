//
//  test.h
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-07-07.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#ifndef Big_Diff_test_h
#define Big_Diff_test_h

class Test {
    
    int buffer_size = 50000;
    std::string small_file = "/tmp/small";
    std::string large_file = "/tmp/large";
    std::string test_output_dir = "/tmp/test_hashes/";
    std::string in_file_path;
    static int CountLines(std::string filepath);
    static void GzipFile(std::string file_to_zip);
    void LookupDiffs(std::ifstream& in, std::unordered_set<std::string>& expected);
    
    public:
        void TestDiffAccuracy( int total_num_lines, int num_diff_lines );
        void TestHashDiffing();
        void TestHashSpeed(int num_lines);
        Test(std::string in_file_path);
};

#endif
