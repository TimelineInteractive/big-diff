//
//  logger.cpp
//  Big Diff
//
//  Created by Ian Chipperfield on 2014-06-23.
//  Copyright (c) 2014 Axiom Zen. All rights reserved.
//

#include "my_logger.h"
#include <boost/log/attributes/timer.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/utility/setup/file.hpp>

#define BOOST_LOG_DYN_LINK

BOOST_LOG_GLOBAL_LOGGER_INIT(my_logger, src::severity_logger_mt)
{
    boost::log::sources::severity_logger_mt< > lg;
    //lg.add_attribute("StopWatch", boost::make_shared< boost::log::attributes::timer >());
    return lg;
}