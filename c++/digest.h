#ifndef DIGEST_H
#define DIGEST_H

#include <string.h>
#include <string>
#include <openssl/md5.h>
#include "MurmurHash3.h"

struct Digest
{
    unsigned char raw_digest[16];
    bool is_added = false;
    Digest() {;}
    Digest(const unsigned char* input) { memcpy( &raw_digest, input, sizeof( raw_digest ) ); }

    inline bool operator<( const Digest& rhs ) const
    {
        unsigned long valueLeft = *(unsigned long*)( &raw_digest[0] );
        unsigned long valueRight = *(unsigned long*)( &rhs.raw_digest[0] );

        if ( valueRight == valueLeft )
        {
            valueLeft = *(long*)( &raw_digest[8] );
            valueRight = *(long*)( &rhs.raw_digest[8] );
        }

        return ( valueLeft < valueRight );
    }

    inline bool operator == ( Digest const& rhs ) const
    {
        unsigned long valueLeft1 = *(unsigned long*)( &raw_digest[0] );
        unsigned long valueRight1 = *(unsigned long*)( &rhs.raw_digest[0] );
        unsigned long valueLeft2 = *(unsigned long*)( &raw_digest[8] );
        unsigned long valueRight2 = *(unsigned long*)( &rhs.raw_digest[8] );

        return ( valueLeft1 == valueRight1 ) &&
               ( valueLeft2 == valueRight2 );
    }

    inline std::string ConvertDigest()
    {
        char buffer[33];
        for (int i=0; i<16; i++)
        {
            sprintf( buffer+i*2, "%02x", raw_digest[i] );
        }
        buffer[32]=0;
        
        return std::string( buffer );
    }
};

// MD5 Hash
 inline void DigestLineMD5( const char* s, unsigned char * digest )
{
    MD5( (const unsigned char*) s, strlen(s), digest );
}

// Murmur3 Hash
inline void DigestLine( const char * s, unsigned char * digest )
{
    MurmurHash3_x64_128( s, strlen(s), 42, digest );
}

#endif
