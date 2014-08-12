//
//  big_diff.h
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-19.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#ifndef Big_Diff_big_diff_h
#define Big_Diff_big_diff_h
#define BOOST_LOG_DYN_LINK

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <boost/log/sources/logger.hpp>
#include "digest.h"

extern std::string file_suffix;
extern std::string old_file_prefix;
extern std::string new_file_prefix;

static int lines_size = 15000000;

#endif
