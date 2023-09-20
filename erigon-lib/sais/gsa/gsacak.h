// vim: noai:ts=2:sw=2

/* 
 * Authors: Felipe A. Louza, Simon Gog, Guilherme P. Telles
 * contact: louza@ic.unicamp.br
 * 03/04/2017
 */

/* 
 * This code is a modification of SACA-K algorithm by G. Nong, which can be
 * retrieved at: http://code.google.com/p/ge-nong/ 
 *
 * Our version of SACA-K, called gSACA-K, maintain the theoretical bounds of the
 * original algorithm to construct the generalized suffix array.
 *
 * Our algorithm gSACA-K can also computes the LCP-array and the Document-array
 * with no additional costs.
 * 
 * gsacak(s, SA, NULL, NULL, n) //computes only SA
 * gsacak(s, SA, LCP,  NULL, n) //computes SA and LCP
 * gsacak(s, SA, NULL, DA, n)   //computes SA and DA
 * gsacak(s, SA, LCP,  DA, n)   //computes SA, LCP and DA
 * 
 */

/******************************************************************************/

#ifndef GSACAK_H
#define GSACAK_H

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>

#define max(a,b) ((a) > (b) ? (a) : (b))

#ifndef DEBUG
  #define DEBUG 0
#endif

#ifndef M64
	#define M64 1
#endif

#if M64
	typedef int64_t	int_t;
	typedef uint64_t uint_t;
	#define PRIdN	PRId64
	#define U_MAX	UINT64_MAX
	#define I_MAX	INT64_MAX
	#define I_MIN	INT64_MIN
#else
	typedef int32_t int_t;
	typedef uint32_t uint_t;
	#define PRIdN	PRId32
	#define U_MAX	UINT32_MAX
	#define I_MAX	INT32_MAX
	#define I_MIN	INT32_MIN
#endif

/*! @option type of s[0,n-1] for integer alphabets 
 *
 *  @constraint sizeof(int_t) >= sizeof(int_text) 
 */
typedef uint32_t int_text;	//4N bytes for s[0..n-1]
#define PRIdT	PRIu32

/*! @option type for array DA
 */
typedef int32_t int_da;
#define PRIdA	PRId32

/******************************************************************************/

/** @brief computes the suffix array of string s[0..n-1] 
 *
 *  @param s	input string with s[n-1]=0
 *  @param SA		suffix array 
 *  @param n	string length
 *  @return -1 if an error occured, otherwise the depth of the recursive calls.
 */
int sacak(unsigned char *s, uint_t *SA, uint_t n);

/** @brief computes the suffix array of string s[0..n-1]
 *
 *  @param k	alphabet size+1 (0 is reserved)
 */
int sacak_int(int_text *s, uint_t *SA, uint_t n, uint_t k);

/******************************************************************************/

/** @brief Computes the suffix array SA (LCP, DA) of T^cat in s[0..n-1]
 *
 *  @param s		input concatenated string, using separators s[i]=1 and with s[n-1]=0
 *  @param SA		Suffix array 
 *  @param LCP	LCP array 
 *  @param DA		Document array
 *  @param n		String length
 *  
 *  @return depth of the recursive calls.
 */
int gsacak(unsigned char *s, uint_t *SA, int_t *LCP, int_da *DA, uint_t n);

/** @brief Computes the suffix array SA (LCP, DA) of T^cat in s[0..n-1]
 *
 *  @param s		input concatenated string, using separators s[i]=1 and with s[n-1]=0
 *  @param SA		Suffix array 
 *  @param LCP	LCP array 
 *  @param DA		Document array
 *  @param n		String length
 *  @param k    alphabet size+2 (0 and 1 are reserved)
 *
 *  @return depth of the recursive calls.
 */
int gsacak_int(int_text *s, uint_t *SA, int_t *LCP, int_da *DA, uint_t n, uint_t k);

/******************************************************************************/



int_t SACA_K(int_t	*s, uint_t *SA,
  uint_t n, unsigned int K,
  uint_t m, int cs, int level);

int_t gSACA_K(uint_t *s, uint_t *SA,
  uint_t n, unsigned int K,
  int cs, uint_t separator, int level);

#endif
