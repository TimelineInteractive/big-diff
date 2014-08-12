bigdiff for C++
===============

Created in Xcode, converted to a Makefile project managed by Eclipse with CDT plugin on Ubuntu14.04

Currently required:
Ubuntu14.04
Eclipse with CDT plugin

Libraries needed (If on Ubuntu, need to apt-get many of these)
g++
boost
zlib
libgopm1
openssl
gzip (currently used in system call)

Linker flags currently being used
-lz
-lgomp
-lcrypto
-lboost_filesystem
-lboost_log_setup
-lpthread
-lboost_system
-lboost_log
-lboost_thread

you may need to link the folder containing the above in your makefile if you are not using eclipse. If you are, these can go under Project -> Properties -> C/C++ Build -> Settings -> GCC C++ Linker -> Libraries (you do not need the -l if you are doing it from here in eclipse)

Other compiler flags
-std=c++0x
-D__GXX_EXPERIMENTAL_CXX0X__

###Running the Binary
####Big Diff:
bigdiff binary expects three arguments: <i>path to old gzip, path to new gzip, output path</i>

####Test:
bigdiff test expects one argument: <i>path to a gzip</i>
