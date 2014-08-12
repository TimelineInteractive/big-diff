//
//  logger.h
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-23.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#ifndef Big_Diff_logger_h
#define Big_Diff_logger_h
#define BOOST_LOG_DYN_LINK

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/record_ostream.hpp>

// Acquire Logger via:
// src::severity_logger_mt< >& lg = my_logger::get();

BOOST_LOG_GLOBAL_LOGGER(my_logger, boost::log::sources::severity_logger_mt<>);

#endif
