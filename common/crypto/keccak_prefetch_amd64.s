#include "textflag.h"

// func prefetchKeccakBucket(b *keccakBucket)
TEXT ·prefetchKeccakBucket(SB), NOSPLIT, $0-8
	MOVQ b+0(FP), AX
	PREFETCHT0 (AX)
	PREFETCHT0 64(AX)
	RET
