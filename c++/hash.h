#ifndef HASH_H
#define HASH_H

#include "digest.h"

struct Hash {
    size_t operator() (const Digest& digest) const
    {
    	std::hash<long> hasher;
    	std::size_t seed = 0;
    	seed ^= hasher(*((unsigned long*)(&digest.raw_digest[0]))) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    	seed ^= hasher(*((unsigned long*)(&digest.raw_digest[8]))) + 0x9e3779b9 + (seed << 6) + (seed >> 2);

        return seed;
    }
};

#endif
