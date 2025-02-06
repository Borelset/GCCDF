/* delta.h
the main fuction is to write a new delta algorithm.
*/

#include <vector>
//#include <assert.h>
#include "htable.h"


#define SUBCHUNK_DEFAULT_SIZE 2*1024
typedef struct {
    //char 		szData[SUBCHUNK_DEFAULT_SIZE];
    u_int64_t nHash; // the chunk fingerprint
    //u_int64_t 	nGear; //the hash of  the last CDC window

    u_int32_t nOffset;//the offset of the string in the chunk
    /* the first character of the string is **buf[nOffset], and
    * there're nOffset characters before the string in the **buf.
    */
    u_int32_t nLength;//string length
    u_int16_t DupFlag; // 1 dup, 0 sim, 2 beg or end
    u_int32_t nSimID; /* used in the input chunk/file to indicate which string in the
						* base chunk/file it is indentical to
						*/
    hlink psNextSubCnk;
} DeltaRecord;// to a string

/* to represent an identical string to the base, 8 bytes, flag=0 */
typedef struct        /* the least write or read unit of disk */
{
    u_int32_t flag_length; //flag & length
    /* the first bit for the flag, the other 31 bits for the length */

    u_int32_t nOffset;
} DeltaUnit1;

/* to represent an  string not identical to the base, 4 bytes, flag=1 */
typedef struct        /* the least write or read unit of disk */
{
    u_int32_t flag_length; //flag & length
    /* the first bit for the flag, the other 31 bits for the length */
} DeltaUnit2;

/* flag=0 for 'D', 1 for 'S' */
void set_flag(void *record, u_int32_t flag);

/* return 0 if flag=0, >0(not 1) if flag=1 */
u_int32_t get_flag(void *record);

void set_length(void *record, u_int32_t length);

u_int32_t get_length(void *record);


typedef struct        /* the new IO unit for delta  */
{
    u_int32_t nDeFlag; //+two bits+: 0, dup, 1 new, 2 reserved for self duplicate...
    u_int32_t nLength; //+14 bits +: the maximal number should be 16384
    u_int32_t nOffset; //+14bits +: the maximal number should be 65536
} DeltaIO;

int my_memcmp(unsigned char *a, unsigned char *b, int length);


int dDelta_Encode(uint8_t *newBuf, u_int32_t newSize,
                  uint8_t *baseBuf, u_int32_t baseSize,
                  uint8_t *deltaBuf, u_int32_t *deltaSize);

int dDelta_Encode_Greedy_v2(uint8_t *newBuf, u_int32_t newSize,
                            uint8_t *baseBuf, u_int32_t baseSize,
                            uint8_t *deltaBuf, u_int32_t *deltaSize);

int eDelta_Encode_v2(uint8_t *newBuf, u_int32_t newSize,
                     uint8_t *baseBuf, u_int32_t baseSize,
                     uint8_t *deltaBuf, u_int32_t *deltaSize,
                     int chunk_number);

int eDelta_Encode_v3(uint8_t *newBuf, u_int32_t newSize,
                     uint8_t *baseBuf, u_int32_t baseSize,
                     uint8_t *deltaBuf, u_int32_t *deltaSize);


/*
int dDelta_Encode_Greedy( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize);	


int eDelta_Encode( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize);	


int eDelta_Encode_v2_test( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize);

int dDelta_Encode_v2( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize);	

int dDelta_Encode_v3( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize, uint32_t bOffset);
*/

int dDelta_Decode(uint8_t *deltaBuf, u_int32_t deltaSize,
                  uint8_t *baseBuf, u_int32_t baseSize,
                  uint8_t *outBuf, u_int32_t *outSize);
/*
int dDelta_Decode_v2( uint8_t* deltaBuf,u_int32_t deltaSize, 
						 uint8_t* baseBuf, u_int32_t baseSize,
						 uint8_t* outBuf,	u_int32_t *outSize);
*/

int Chunking(unsigned char *data, int len,
             DeltaRecord *subChunkLink);

int Chunking_v2(unsigned char *data, int num_of_chunks,
                DeltaRecord *subChunkLink);

int Chunking_v3(unsigned char *data, int len, int num_of_chunks,
                DeltaRecord *subChunkLink);
							
uint64_t weakHash(unsigned char *buf, int len);

void TESTdDelta(char baseBuf[], char diffBuf[][16384], int blkSize = 8);

void TESTbenchMark();

void TESTTARFile(char *InputFile, char *BaseFile, int flag);


int Chunking_v5(unsigned char *data, int len, DeltaRecord *subChunkLink);

int dDelta_Encode_Greedy_v5(uint8_t *newBuf, u_int32_t newSize,
                            uint8_t *baseBuf, u_int32_t baseSize,
                            uint8_t *deltaBuf, u_int32_t *deltaSize);

int eDelta_Encode_v5(uint8_t *newBuf, u_int32_t newSize,
                     uint8_t *baseBuf, u_int32_t baseSize,
                     uint8_t *deltaBuf, u_int32_t *deltaSize);

int Chunking_v51(unsigned char *data, int len, int num_of_chunks, DeltaRecord *subChunkLink);
