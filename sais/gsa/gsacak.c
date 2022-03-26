// vim: noai:ts=2:sw=2

#include "gsacak.h"

// set only the highest bit as 1, i.e. 1000...
//const unsigned int EMPTY_k=((unsigned int)1)<<(sizeof(unsigned int)*8-1); 
const uint_t EMPTY_k=((uint_t)1)<<(sizeof(uint_t)*8-1); 

// get s[i] at a certain level
#define chr(i) (cs==sizeof(int_t)?((int_t*)s)[i]:(cs==sizeof(int_text)?((int_text*)s)[i]:((unsigned char *)s)[i]))

#define true 1
#define false 0

#define DEPTH 0  // compute time and size of reduced problem for each recursion call
#define PHASES 0 // compute time for each phase
#define RMQ_L   2  //variants = (1, trivial) (2, using Gog's stack)
#define RMQ_S   2  //variants = (1, trivial) (2, using Gog's stack)
 
#define STACK_SIZE_L 894 //to use 10Kb of working space
#define STACK_SIZE_S 894 //to use 10Kb of working space

#define EMPTY_STRING 0 //check if there is an empty string in the input collection

typedef struct _pair{
  uint_t idx;
  int_t lcp;
} t_pair_k;

int compare_k (const void * a, const void * b){
  if(*(const uint_t *)a < *(const uint_t *)b) return -1;
  if(*(const uint_t *)a > *(const uint_t *)b) return 1;
return 0;
}

void stack_push_k(t_pair_k* STACK, int_t *top, uint_t idx, int_t lcp){

  STACK[*top].idx=idx;
  STACK[*top].lcp=lcp;

  (*top)++;
}

void compute_lcp_phi_sparse(int_t *s, uint_t *SA1, 
  uint_t *RA, int_t *LCP, int_t *PLCP,
  uint_t n1, int cs, uint_t separator) {

  uint_t i;

  PLCP[SA1[0]]=0;//PLCP* (lms) is stored in PLCP array
  for(i=1; i<n1; i++)
    PLCP[SA1[i]] = LCP[i]; 

  LCP[SA1[0]]=0;//PHI is stored in LCP array
  for(i=1; i<n1; i++)
    LCP[SA1[i]] = SA1[i-1]; //RA[SA1[i-1]];

  int_t l=0; //q=0;
  for(i=0; i<n1-1;i++){
    if(chr(RA[i])==separator) continue;

    l = max(PLCP[i], l);//consider the LCP-value of the lms-substrings

    while(chr(RA[i]+l)==chr(RA[LCP[i]]+l) && !(chr(RA[i]+l) == separator && chr(RA[LCP[i]]+l)==separator) ) ++l;
    PLCP[i]=l;

    if(LCP[i]==n1-1) l -= RA[i+1]-RA[i];
    else l -= max(RA[i+1]-RA[i], RA[LCP[i]+1]-RA[LCP[i]]);//LCP[i] stores the distance of i-th suffix to its successor
  }

  LCP[0]=0;
  for(i=1; i<n1;i++) LCP[i]=PLCP[SA1[i]];

}

/*****************************************************************************/

void getBuckets_k(int_t *s, 
  uint_t *bkt, uint_t n,
  unsigned int K, int end, int cs) { 
  uint_t i, sum=0;
  
  // clear all buckets .
  for(i=0; i<K; i++) bkt[i]=0; 
  
  // compute the size of each bucket .
  for(i=0; i<n; i++) bkt[chr(i)]++; 
  
  for(i=0; i<K; i++) { 
    sum+=bkt[i]; 
    bkt[i]=end ? sum-1 : sum-bkt[i]; 
  }
}

/*****************************************************************************/

void putSuffix0(uint_t *SA, 
  int_t *s, uint_t *bkt, 
  uint_t n, unsigned int K, int_t n1, int cs) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k(s, bkt, n, K, true, cs);

  // put the suffixes into their buckets.
  for(i=n1-1; i>0; i--) {
    j=SA[i]; SA[i]=0;
    SA[bkt[chr(j)]--]=j;
  }
  SA[0]=n-1; // set the single sentinel suffix.
}

void putSuffix0_generalized(uint_t *SA, 
  uint_t *s, uint_t *bkt, 
  uint_t n, unsigned int K, int_t n1, int cs, uint_t separator) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  int_t tmp=bkt[separator]--;// shifts one position left of bkt[separator]

  // put the suffixes into their buckets.
  for(i=n1-1; i>0; i--) {
    j=SA[i]; SA[i]=0;
    SA[bkt[chr(j)]--]=j;
  }

 // SA[0]=n-1; // set the single sentinel suffix.

  SA[tmp]=SA[0]-1;// insert the last separator at the end of bkt[separator]

}

void putSuffix0_generalized_LCP(uint_t *SA, int_t *LCP,
  uint_t *s, uint_t *bkt, 
  uint_t n, unsigned int K, int_t n1, int cs, uint_t separator) {
  uint_t i, j;
  int_t l;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  int_t tmp=bkt[separator]--;// shifts one position left of bkt[separator]

  // put the suffixes into their buckets.
  for(i=n1-1; i>0; i--) {
    j=SA[i]; SA[i]=U_MAX;
    l=LCP[i]; LCP[i]=0;

    SA[bkt[chr(j)]]=j;
    LCP[bkt[chr(j)]--]=l;
  }

 // SA[0]=n-1; // set the single sentinel suffix.

  SA[tmp]=SA[0]-1;// insert the last separator at the end of bkt[separator]

}

void putSuffix0_generalized_DA(uint_t *SA, int_da *DA,
  uint_t *s, uint_t *bkt, 
  uint_t n, unsigned int K, int_t n1, int cs, uint_t separator) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  int_t tmp=bkt[separator]--;// shifts one position left of bkt[separator]
  
  // put the suffixes into their buckets.
  for(i=n1-1; i>0; i--) {
    j=SA[i]; SA[i]=0;
    SA[bkt[chr(j)]]=j;
    DA[bkt[chr(j)]--]=DA[i];
  }

 // SA[0]=n-1; // set the single sentinel suffix.

  SA[tmp]=SA[0]-1;// insert the last separator at the end of bkt[separator]
  DA[tmp]= (int_da) tmp-1;

}


void putSuffix0_generalized_LCP_DA(uint_t *SA, int_t *LCP,  int_da *DA, 
  uint_t *s, uint_t *bkt, 
  uint_t n, unsigned int K, int_t n1, int cs, uint_t separator) {
  uint_t i, j;
  int_t l;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  int_t tmp=bkt[separator]--;// shifts one position left of bkt[separator]

  // put the suffixes into their buckets.
  for(i=n1-1; i>0; i--) {
    j=SA[i]; SA[i]=U_MAX;
    l=LCP[i]; LCP[i]=0;

    SA[bkt[chr(j)]]=j;
    DA[bkt[chr(j)]]=DA[i];
    LCP[bkt[chr(j)]--]=l;
  }

 // SA[0]=n-1; // set the single sentinel suffix.

  SA[tmp]=SA[0]-1;// insert the last separator at the end of bkt[separator]
  DA[tmp]= (int_da) tmp-1;
}

/*****************************************************************************/

void induceSAl0(uint_t *SA,
  int_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int_t suffix, int cs) {
  uint_t i, j;

  // find the head of each bucket.
  getBuckets_k(s, bkt, n, K, false, cs);

  bkt[0]++; // skip the virtual sentinel.
  for(i=0; i<n; i++)
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)>=chr(j+1)) {
        SA[bkt[chr(j)]]=j;
        bkt[chr(j)]++;
        if(!suffix && i>0) SA[i]=0;
      }
    }
}

void induceSAs0(uint_t *SA,
  int_t	*s, uint_t *bkt,
  uint_t n, unsigned int K, int_t suffix, int cs) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k(s, bkt, n, K, true, cs);

  for(i=n-1; i>0; i--)
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)<=chr(j+1) && bkt[chr(j)]<i) {
        SA[bkt[chr(j)]]=j;
        bkt[chr(j)]--;
        if(!suffix) SA[i]=0;
      }
    }
}
/*****************************************************************************/

void induceSAl0_generalized(uint_t *SA,
  uint_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int_t suffix, int cs, uint_t separator) {
  uint_t i, j;

  // find the head of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, false, cs);

  bkt[0]++; // skip the virtual sentinel.
  for(i=0; i<n; i++)
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)>=chr(j+1) ) {
	if(chr(j)!=separator)//gsa-is
          SA[bkt[chr(j)]++]=j;
        if(!suffix && i>0) SA[i]=0;
      }
    }
}

void induceSAs0_generalized(uint_t *SA,
  uint_t *s, uint_t *bkt,
  uint_t n, uint_t K, int_t suffix, int cs, uint_t separator) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  for(i=n-1; i>0; i--)
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)<=chr(j+1) && bkt[chr(j)]<i) {
        if(chr(j)!=separator)
	  SA[bkt[chr(j)]--]=j;
        if(!suffix) SA[i]=0;
      }
    }
}

/*****************************************************************************/

void induceSAl0_generalized_LCP(uint_t *SA, int_t *LCP,
  uint_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int cs, uint_t separator) {
  uint_t i, j;

  for(i=0;i<K;i++)
    if(bkt[i]+1<n) if(SA[bkt[i]+1]!=U_MAX) LCP[bkt[i]+1]=I_MIN;

  // find the head of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, false, cs);

  for(i=0;i<K;i++)
    if(bkt[i]<n) LCP[bkt[i]]=-2;

  #if RMQ_L == 1 
  int_t *M=(int_t *)malloc(sizeof(int_t)*K);
  for(i=0;i<K;i++){
    M[i]=I_MAX;
  }
  #elif RMQ_L == 2
  uint_t* last_occ = (uint_t*) malloc(K*sizeof(uint_t));
  uint_t* tmp = (uint_t*) malloc(K*sizeof(uint_t));

  t_pair_k* STACK = (t_pair_k*) malloc((STACK_SIZE_L+2)*sizeof(t_pair_k));
  int_t top = 0;
  stack_push_k(STACK, &top, 0, -1);//init
  for(i=0;i<K;i++) last_occ[i]=0;
  #endif 

  #if DEBUG
  printf("inducing..\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  bkt[0]++; // skip the virtual sentinel.
  for(i=0; i<n; i++){
    if(SA[i]!=U_MAX){

      if(LCP[i]==I_MIN){ //is a L/S-seam position
  	  int_t l=0;
	  if(SA[bkt[chr(SA[i])]-1]<n-1)	
   	    while(chr(SA[i]+l)==chr(SA[bkt[chr(SA[i])]-1]+l))++l;
  	  LCP[i]=l;
      }
      #if RMQ_L == 1
        uint_t k;
        for(k=0; k<K; k++) if(M[k]>LCP[i]) M[k] = max(0,LCP[i]);
      #elif RMQ_L == 2
        int_t min_lcp=0;
        uint_t last;

        if(!SA[i]) last = 0;
        else{
          last = last_occ[chr(SA[i]-1)];
          last_occ[chr(SA[i]-1)] = i+1;
        }
 
        int_t lcp=max(0,LCP[i]);
        while(STACK[(top)-1].lcp>=lcp) (top)--;	

        stack_push_k(STACK, &top, i+1, lcp);
        j = top-1;

        while(STACK[j].idx>last) j--;
        min_lcp=STACK[(j+1)].lcp;

      #endif

      if(SA[i]>0) {
        j=SA[i]-1;
        if(chr(j)>=chr(j+1))
  	if(chr(j)!=separator){//gsa-is
            SA[bkt[chr(j)]]=j;

            #if RMQ_L == 1
  	      LCP[bkt[chr(j)]]+=M[chr(j)]+1;
    	      M[chr(j)] = I_MAX;
            #elif RMQ_L == 2
  	      LCP[bkt[chr(j)]]+=min_lcp+1; 
            #endif
  
            bkt[chr(j)]++;
	}
      	if(bkt[chr(SA[i])]-1<i){ //if is LMS-type
	  if(chr(SA[i])!=separator)
	  SA[i]=U_MAX;
        }

      }
      #if RMQ_L == 2
      if(top>STACK_SIZE_L){//if stack is full

        int_t j;
        memcpy(tmp, last_occ, K*sizeof(uint_t));
        qsort(tmp, K, sizeof(uint_t), compare_k);
       
        int_t curr=1, end=1;
        STACK[top].idx=U_MAX;
        for(j=0;j<K; j++){

          if(STACK[end-1].idx < tmp[j]+1){

            while(STACK[curr].idx<tmp[j]+1) curr++;

            if(curr<top){
	      stack_push_k(STACK, &end, STACK[curr].idx, STACK[curr].lcp);
	      curr++;
            }
	  }
        }
 
        if(end>=STACK_SIZE_L){
          fprintf(stderr,"ERROR: induceSAl0_LCP\n");
          exit(1);
        }

        top = end;
      }
      #endif
    }
  }
  #if RMQ_L == 1
  free(M);
  #elif RMQ_L == 2
  free(STACK);
  free(last_occ);
  free(tmp);
  #endif

}

void induceSAs0_generalized_LCP(uint_t *SA, int_t* LCP,
  uint_t *s, uint_t *bkt,
  uint_t n, uint_t K, int cs, uint_t separator) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  #if RMQ_S == 1
  int_t *M=(int_t *)malloc(sizeof(int_t)*K);
  for(i=0;i<K;i++) M[i]=I_MAX;
  #elif RMQ_S == 2 
  uint_t* last_occ = (uint_t*) malloc(K*sizeof(uint_t));
  uint_t* tmp = (uint_t*) malloc(K*sizeof(uint_t));

  t_pair_k* STACK = (t_pair_k*) malloc((STACK_SIZE_S+2)*sizeof(t_pair_k));
  int_t top = 0;
  stack_push_k(STACK, &top, n, -1); //init
  for(i=0;i<K;i++) last_occ[i]=n-1;
  #endif

  for(i=n-1; i>0; i--){
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)<=chr(j+1) && bkt[chr(j)]<i)// induce S-type
        if(chr(j)!=separator){
	  SA[bkt[chr(j)]]=j;

          #if RMQ_S == 1
  	    if(LCP[bkt[chr(j)]+1]>=0) 
  	      LCP[bkt[chr(j)]+1]=M[chr(j)]+1;
  	  
	    if(LCP[bkt[chr(j)]]>0) 
  	      LCP[bkt[chr(j)]]=I_MAX;

          #elif RMQ_S == 2
            int_t min = I_MAX, end = top-1; 
  
  	    int_t last=last_occ[chr(j)];
            while(STACK[end].idx<=last) end--;
  
            min=STACK[(end+1)].lcp;
            last_occ[chr(j)] = i;
  
  	    if(LCP[bkt[chr(j)]+1]>=0) 
              LCP[bkt[chr(j)]+1]=min+1;
          #endif
  

          #if RMQ_S == 1
  	  M[chr(j)] = I_MAX;
          #endif
  
          bkt[chr(j)]--;
 
  	  if(SA[bkt[chr(j)]]!=U_MAX) {//L/S-seam
            int_t l=0;	
            while(chr(SA[bkt[chr(j)]+1]+l)==chr(SA[bkt[chr(j)]]+l))++l;
            LCP[bkt[chr(j)]+1]=l;
  	  }
  	}
     }

    if(LCP[i]<0) LCP[i]=0;
    #if RMQ_S == 1
      int_t k;
      for(k=0; k<K; k++) if(M[k]>LCP[i]) M[k] = LCP[i];
    #elif RMQ_S == 2

      int_t lcp=max(0,LCP[i]);

      while(STACK[(top)-1].lcp>=lcp) (top)--;
      stack_push_k(STACK, &top, i, lcp);

      if(top>=STACK_SIZE_S){

          int_t j;
          memcpy(tmp, last_occ, K*sizeof(uint_t));
          qsort(tmp, K, sizeof(uint_t), compare_k);

          int_t curr=1, end=1;

	   for(j=K-1;j>=0; j--){

            if(tmp[j] < STACK[end-1].idx){

  	      while(STACK[curr].idx>tmp[j] && curr < top) curr++;
	      if(curr>=top) break;
	      stack_push_k(STACK, &end, STACK[curr].idx, STACK[curr].lcp);
	      curr++;
            }
          } 

          if(end>=STACK_SIZE_S){
            fprintf(stderr,"ERROR: induceSAl0_LCP\n");
            exit(1);
          }
          top = end;
      }
      #endif

  }

  LCP[0]=0;

  //variant 1
  #if RMQ_S == 1
  free(M);
  #elif RMQ_S == 2
  free(STACK);
  free(last_occ);
  free(tmp);
  #endif
}


/*****************************************************************************/

void induceSAl0_generalized_DA(uint_t *SA, int_da* DA,
  uint_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int cs, uint_t separator) {
  uint_t i, j;

  // find the head of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, false, cs);

  bkt[0]++; // skip the virtual sentinel.
  for(i=0; i<n; i++)
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)>=chr(j+1) ) {
	    if(chr(j)!=separator){//gsa-is
          SA[bkt[chr(j)]]=j;
          DA[bkt[chr(j)]++]=DA[i];
	    }
      }
    }
}

void induceSAs0_generalized_DA(uint_t *SA, int_da* DA,
  uint_t *s, uint_t *bkt,
  uint_t n, uint_t K, int cs, uint_t separator) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  for(i=n-1; i>0; i--)
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)<=chr(j+1) && bkt[chr(j)]<i) {
        if(chr(j)!=separator){
		  SA[bkt[chr(j)]]=j;
		  DA[bkt[chr(j)]--]=DA[i];
	    }
      }
    }
}

/*****************************************************************************/

void induceSAl0_generalized_LCP_DA(uint_t *SA, int_t *LCP, int_da *DA,
  uint_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int cs, uint_t separator) {
  uint_t i, j;

  for(i=0;i<K;i++)
    if(bkt[i]+1<n) if(SA[bkt[i]+1]!=U_MAX) LCP[bkt[i]+1]=I_MIN;

  // find the head of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, false, cs);

  for(i=0;i<K;i++)
    if(bkt[i]<n) LCP[bkt[i]]=-2;

  #if RMQ_L == 1 
  int_t *M=(int_t *)malloc(sizeof(int_t)*K);
  for(i=0;i<K;i++){
    M[i]=I_MAX;
  }
  #elif RMQ_L == 2
  uint_t* last_occ = (uint_t*) malloc(K*sizeof(uint_t));
  uint_t* tmp = (uint_t*) malloc(K*sizeof(uint_t));

  t_pair_k* STACK = (t_pair_k*) malloc((STACK_SIZE_L+2)*sizeof(t_pair_k));
  int_t top = 0;
  stack_push_k(STACK, &top, 0, -1);//init
  for(i=0;i<K;i++) last_occ[i]=0;
  #endif 

  #if DEBUG
  printf("inducing..\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  bkt[0]++; // skip the virtual sentinel.
  for(i=0; i<n; i++){
    if(SA[i]!=U_MAX){

      if(LCP[i]==I_MIN){ //is a L/S-seam position
  	  int_t l=0;
		  if(SA[bkt[chr(SA[i])]-1]<n-1)	
   	    while(chr(SA[i]+l)==chr(SA[bkt[chr(SA[i])]-1]+l))++l;
  	  LCP[i]=l;
      }
      #if RMQ_L == 1
        uint_t k;
        for(k=0; k<K; k++) if(M[k]>LCP[i]) M[k] = max(0,LCP[i]);
      #elif RMQ_L == 2
        int_t min_lcp=0;
        uint_t last;

        if(!SA[i]) last = 0;
        else{
          last = last_occ[chr(SA[i]-1)];
          last_occ[chr(SA[i]-1)] = i+1;
        }
 
        int_t lcp=max(0,LCP[i]);
        while(STACK[(top)-1].lcp>=lcp) (top)--;	

        stack_push_k(STACK, &top, i+1, lcp);
        j = top-1;

        while(STACK[j].idx>last) j--;
        min_lcp=STACK[(j+1)].lcp;

      #endif

      if(SA[i]>0) {
        j=SA[i]-1;
        if(chr(j)>=chr(j+1))
					if(chr(j)!=separator){//gsa-is
            SA[bkt[chr(j)]]=j;
            DA[bkt[chr(j)]]=DA[i];

            #if RMQ_L == 1
							LCP[bkt[chr(j)]]+=M[chr(j)]+1;
	    	      M[chr(j)] = I_MAX;
            #elif RMQ_L == 2
				      LCP[bkt[chr(j)]]+=min_lcp+1; 
            #endif
  
            bkt[chr(j)]++;
				}
      	if(bkt[chr(SA[i])]-1<i){ //if is LMS-type
					if(chr(SA[i])!=separator)
					SA[i]=U_MAX;
        }
      }
      #if RMQ_L == 2
      if(top>STACK_SIZE_L){//if stack is full

        int_t j;
        memcpy(tmp, last_occ, K*sizeof(uint_t));
        qsort(tmp, K, sizeof(uint_t), compare_k);
       
        int_t curr=1, end=1;
        STACK[top].idx=U_MAX;
        for(j=0;j<K; j++){

          if(STACK[end-1].idx < tmp[j]+1){

            while(STACK[curr].idx<tmp[j]+1) curr++;

            if(curr<top){
	      stack_push_k(STACK, &end, STACK[curr].idx, STACK[curr].lcp);
	      curr++;
            }
	  }
        }
 
        if(end>=STACK_SIZE_L){
          fprintf(stderr,"ERROR: induceSAl0_LCP\n");
          exit(1);
        }

        top = end;
      }
      #endif
    }
  }
  #if RMQ_L == 1
  free(M);
  #elif RMQ_L == 2
  free(STACK);
  free(last_occ);
  free(tmp);
  #endif

}

void induceSAs0_generalized_LCP_DA(uint_t *SA, int_t* LCP, int_da* DA,
  uint_t *s, uint_t *bkt,
  uint_t n, uint_t K, int cs, uint_t separator) {
  uint_t i, j;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  #if RMQ_S == 1
  int_t *M=(int_t *)malloc(sizeof(int_t)*K);
  for(i=0;i<K;i++) M[i]=I_MAX;
  #elif RMQ_S == 2 
  uint_t* last_occ = (uint_t*) malloc(K*sizeof(uint_t));
  uint_t* tmp = (uint_t*) malloc(K*sizeof(uint_t));

  t_pair_k* STACK = (t_pair_k*) malloc((STACK_SIZE_S+2)*sizeof(t_pair_k));
  int_t top = 0;
  stack_push_k(STACK, &top, n, -1); //init
  for(i=0;i<K;i++) last_occ[i]=n-1;
  #endif

  for(i=n-1; i>0; i--){
    if(SA[i]>0) {
      j=SA[i]-1;
      if(chr(j)<=chr(j+1) && bkt[chr(j)]<i)// induce S-type
      if(chr(j)!=separator){
	    SA[bkt[chr(j)]]=j;
        DA[bkt[chr(j)]]=DA[i];
      
        #if RMQ_S == 1
  	    if(LCP[bkt[chr(j)]+1]>=0) 
  	      LCP[bkt[chr(j)]+1]=M[chr(j)]+1;

	    if(LCP[bkt[chr(j)]]>0) 
  	      LCP[bkt[chr(j)]]=I_MAX;

        #elif RMQ_S == 2
          int_t min = I_MAX, end = top-1; 
  
          int_t last=last_occ[chr(j)];
          while(STACK[end].idx<=last) end--;
  
          min=STACK[(end+1)].lcp;
          last_occ[chr(j)] = i;
  
  	      if(LCP[bkt[chr(j)]+1]>=0) 
            LCP[bkt[chr(j)]+1]=min+1;
        #endif
  

        #if RMQ_S == 1
          M[chr(j)] = I_MAX;
        #endif
  
        bkt[chr(j)]--;
 
        if(SA[bkt[chr(j)]]!=U_MAX) {//L/S-seam
              int_t l=0;	
              while(chr(SA[bkt[chr(j)]+1]+l)==chr(SA[bkt[chr(j)]]+l))++l;
              LCP[bkt[chr(j)]+1]=l;
        }
      }
    }

    if(LCP[i]<0) LCP[i]=0;
    #if RMQ_S == 1
      int_t k;
      for(k=0; k<K; k++) if(M[k]>LCP[i]) M[k] = LCP[i];
    #elif RMQ_S == 2

      int_t lcp=max(0,LCP[i]);

      while(STACK[(top)-1].lcp>=lcp) (top)--;
      stack_push_k(STACK, &top, i, lcp);

      if(top>=STACK_SIZE_S){

          int_t j;
          memcpy(tmp, last_occ, K*sizeof(uint_t));
          qsort(tmp, K, sizeof(uint_t), compare_k);

          int_t curr=1, end=1;

	   for(j=K-1;j>=0; j--){

            if(tmp[j] < STACK[end-1].idx){

  	      while(STACK[curr].idx>tmp[j] && curr < top) curr++;
	      if(curr>=top) break;
	      stack_push_k(STACK, &end, STACK[curr].idx, STACK[curr].lcp);
	      curr++;
            }
          } 

          if(end>=STACK_SIZE_S){
            fprintf(stderr,"ERROR: induceSAl0_LCP\n");
            exit(1);
          }
          top = end;
      }
      #endif

  }

  LCP[0]=0;

  //variant 1
  #if RMQ_S == 1
  free(M);
  #elif RMQ_S == 2
  free(STACK);
  free(last_occ);
  free(tmp);
  #endif
}


/*****************************************************************************/


void putSubstr0(uint_t *SA,
  int_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int cs) {
  uint_t i, cur_t, succ_t;

  // find the end of each bucket.
  getBuckets_k(s, bkt, n, K, true, cs);

  // set each item in SA as empty.
  for(i=0; i<n; i++) SA[i]=0;

  succ_t=0; // s[n-2] must be L-type.
  for(i=n-2; i>0; i--) {
    cur_t=(chr(i-1)<chr(i) ||
           (chr(i-1)==chr(i) && succ_t==1)
          )?1:0;
    if(cur_t==0 && succ_t==1) SA[bkt[chr(i)]--]=i;
    succ_t=cur_t;
  }

  // set the single sentinel LMS-substring.
  SA[0]=n-1;
}

/*****************************************************************************/
void putSubstr0_generalized(uint_t *SA,
  uint_t *s, uint_t *bkt,
  uint_t n, unsigned int K, int cs, uint_t separator) {
  uint_t i, cur_t, succ_t;

  // find the end of each bucket.
  getBuckets_k((int_t*)s, bkt, n, K, true, cs);

  // set each item in SA as empty.
  for(i=0; i<n; i++) SA[i]=0;

  // gsa-is
  int_t tmp=bkt[separator]--;// shifts one position left of bkt[separator]
  
  SA[0]=n-1;// set the single sentinel LMS-substring.

  SA[tmp] = SA[0]-1; // insert the last separator at the end of bkt[separator]

  int_t p=n-2;

  succ_t=0; // s[n-2] must be L-type.
  for(i=n-2; i>0; i--) {
    cur_t=(chr(i-1)<chr(i) ||
           (chr(i-1)==chr(i) && succ_t==1)
          )?1:0;
    if(cur_t==0 && succ_t==1){

      if(chr(i)==separator)
        SA[++bkt[chr(p)]]=0; // removes LMS-positions that induces separator suffixes

      SA[bkt[chr(i)]--]=i;
      p=i;
    }
    succ_t=cur_t;
  }
}
/*****************************************************************************/

void putSuffix1(int_t *SA, int_t *s, int_t n1, int cs) {
  int_t i, j, pos=n1, cur, pre=-1;
  
  for(i=n1-1; i>0; i--) {
    j=SA[i]; SA[i]=EMPTY_k;
    cur=chr(j);
    if(cur!=pre) {
      pre=cur; pos=cur;
    }
    SA[pos--]=j;
  }
}

void induceSAl1(int_t *SA, int_t *s, 
  int_t n, int_t suffix, int cs) {
  int_t h, i, j, step=1;
  
  for(i=0; i<n; i+=step) {
    step=1; j=SA[i]-1;
    if(SA[i]<=0) continue;
    int_t c=chr(j), c1=chr(j+1);
    int_t isL=c>=c1;
    if(!isL) continue;

    // s[j] is L-type.

    int_t d=SA[c];
    if(d>=0) {
      // SA[c] is borrowed by the left
      //   neighbor bucket.
      // shift-left the items in the
      //   left neighbor bucket.
      int_t foo, bar;
      foo=SA[c];
      for(h=c-1; SA[h]>=0||SA[h]==EMPTY_k; h--)
      { bar=SA[h]; SA[h]=foo; foo=bar; }
      SA[h]=foo;
      if(h<i) step=0;

      d=EMPTY_k;
    }

    if(d==EMPTY_k) { // SA[c] is empty.
      if(c<n-1 && SA[c+1]==EMPTY_k) {
        SA[c]=-1; // init the counter.
        SA[c+1]=j;
      }
      else        
        SA[c]=j; // a size-1 bucket.
    }
    else { // SA[c] is reused as a counter.
        int_t pos=c-d+1;
        if(pos>n-1 || SA[pos]!=EMPTY_k) {
          // we are running into the right
          //   neighbor bucket.
          // shift-left one step the items
          //   of bucket(SA, S, j).
          for(h=0; h<-d; h++)
            SA[c+h]=SA[c+h+1];
          pos--;
          if(c<i) step=0;
        }
        else
          SA[c]--;

        SA[pos]=j;
    }

    int_t c2;
    int_t isL1=(j+1<n-1) && (c1>(c2=chr(j+2)) || (c1==c2 && c1<i));  // is s[SA[i]] L-type?
    if((!suffix || !isL1) && i>0) {
      int_t i1=(step==0)?i-1:i;
      SA[i1]=EMPTY_k;
    }
  }

  // scan to shift-left the items in each bucket 
  //   with its head being reused as a counter.
  for(i=1; i<n; i++) {
    j=SA[i];
    if(j<0 && j!=EMPTY_k) { // is SA[i] a counter?
      for(h=0; h<-j; h++)
        SA[i+h]=SA[i+h+1];
      SA[i+h]=EMPTY_k;
    }
  }
}

void induceSAs1(int_t *SA, int_t *s, 
  int_t n, int_t suffix, int cs) {
  int_t h, i, j, step=1;
  
  for(i=n-1; i>0; i-=step) {
    step=1; j=SA[i]-1;
    if(SA[i]<=0) continue;
    int_t c=chr(j), c1=chr(j+1);
    int_t isS=(c<c1) || (c==c1 && c>i);
    if(!isS) continue;

    // s[j] is S-type

    int_t d=SA[c];
    if(d>=0) {
      // SA[c] is borrowed by the right
      //   neighbor bucket.
      // shift-right the items in the
      //   right neighbor bucket.
      int_t foo, bar;
      foo=SA[c];
      for(h=c+1; SA[h]>=0||SA[h]==EMPTY_k; h++)
      { bar=SA[h]; SA[h]=foo; foo=bar; }
      SA[h]=foo;
      if(h>i) step=0;

      d=EMPTY_k;
    }

    if(d==EMPTY_k) { // SA[c] is empty.
      if(SA[c-1]==EMPTY_k) {
        SA[c]=-1; // init the counter.
        SA[c-1]=j;
      }
      else
        SA[c]=j; // a size-1 bucket.
    }
    else { // SA[c] is reused as a counter.
        int_t pos=c+d-1;
        if(SA[pos]!=EMPTY_k) {
          // we are running into the left
          //   neighbor bucket.
          // shift-right one step the items 
          //   of bucket(SA, S, j).
          for(h=0; h<-d; h++)
            SA[c-h]=SA[c-h-1];
          pos++;
          if(c>i) step=0;
        }
        else
          SA[c]--;

        SA[pos]=j;
    }

    if(!suffix) {
      int_t i1=(step==0)?i+1:i;
      SA[i1]=EMPTY_k;
    }
  }

  // scan to shift-right the items in each bucket
  //   with its head being reused as a counter.
  if(!suffix)
    for(i=n-1; i>0; i--) {
      j=SA[i];
      if(j<0 && j!=EMPTY_k) { // is SA[i] a counter?
        for(h=0; h<-j; h++)
          SA[i-h]=SA[i-h-1];
        SA[i-h]=EMPTY_k;
      }
    }
}

void putSubstr1(int_t *SA, int_t *s, int_t n, int cs) {
  int_t h, i, j;

  for(i=0; i<n; i++) SA[i]=EMPTY_k;

  int_t c, c1, t, t1;
  c1=chr(n-2);
  t1=0; 
  for(i=n-2; i>0; i--) {
    c=c1; t=t1; 
    c1=chr(i-1);
    t1=c1<c || (c1==c && t);
    if(t && !t1) {
      if(SA[c]>=0) {
        // SA[c] is borrowed by the right
        //   neighbor bucket.
        // shift-right the items in the
        //   right neighbor bucket.
        int_t foo, bar;
        foo=SA[c];
        for(h=c+1; SA[h]>=0; h++)
        { bar=SA[h]; SA[h]=foo; foo=bar; }
        SA[h]=foo;

        SA[c]=EMPTY_k;
      }

      int_t d=SA[c];
      if(d==EMPTY_k) { // SA[c] is empty.
        if(SA[c-1]==EMPTY_k) {
          SA[c]=-1; // init the counter.
          SA[c-1]=i;
        }
        else
          SA[c]=i; // a size-1 bucket.
      }
      else { // SA[c] is reused as a counter
          int_t pos=c+d-1;
          if(SA[pos]!=EMPTY_k) {
            // we are running into the left
            //   neighbor bucket.
            // shift-right one step the items 
            //   of bucket(SA, S, i).
            for(h=0; h<-d; h++)
              SA[c-h]=SA[c-h-1];
            pos++;
          }
          else
            SA[c]--;

          SA[pos]=i;
      }
    }
  }

  // scan to shift-right the items in each bucket
  //   with its head being reused as a counter.
  for(i=n-1; i>0; i--) {
    j=SA[i];
    if(j<0 && j!=EMPTY_k) { // is SA[i] a counter?
      for(h=0; h<-j; h++)
        SA[i-h]=SA[i-h-1];
      SA[i-h]=EMPTY_k;
    }
  }

  // put the single sentinel LMS-substring.
  SA[0]=n-1;
}

uint_t getLengthOfLMS(int_t	*s, 
  uint_t n, int level, uint_t x, int cs) {
  if(x==n-1) return 1;  
  
  uint_t dist=0, i=1;  
  while(1) {
    if(chr(x+i)<chr(x+i-1)) break;
    i++;
  }  
  while(1) {
    if(x+i>n-1 || chr(x+i)>chr(x+i-1)) break;
    if(x+i==n-1 || chr(x+i)<chr(x+i-1)) dist=i;
    i++;
  }
  
  return dist+1;
}

uint_t nameSubstr(uint_t *SA, 
  int_t *s, uint_t *s1, uint_t n, 
  uint_t m, uint_t n1, int level, int cs) {
  uint_t i, j, cur_t, succ_t;

  // init the name array buffer
  for(i=n1; i<n; i++) SA[i]=EMPTY_k;

  // scan to compute the interim s1
  uint_t name=0, name_ctr=0;
  uint_t pre_pos=n-1, pre_len=0;
  for(i=0; i<n1; i++) {
    int diff=false;
    uint_t len, pos=SA[i];

    uint_t d;
    len=getLengthOfLMS(s, n, level, pos, cs);
    if(len!=pre_len) diff=true;
    else
      for(d=0; d<len; d++)
        if(pos+d==n-1 || pre_pos+d==n-1 ||
           chr(pos+d)!=chr(pre_pos+d)) {
          diff=true; break;
        }

    if(diff) {
      name=i; name_ctr++;
      SA[name]=1; // a new name.
      pre_pos=pos; pre_len=len;
    }
    else
      SA[name]++; // count this name.

    SA[n1+pos/2]=name;
  }

  // compact the interim s1 sparsely stored 
  //   in SA[n1, n-1] into SA[m-n1, m-1].
  for(i=n-1, j=m-1; i>=n1; i--)
    if(SA[i]!=EMPTY_k) SA[j--]=SA[i];

  // rename each S-type character of the
  //   interim s1 as the end of its bucket
  //   to produce the final s1.
  succ_t=1;
  for(i=n1-1; i>0; i--) {
    int_t ch=s1[i], ch1=s1[i-1];
    cur_t=(ch1< ch || (ch1==ch && succ_t==1))?1:0;
    if(cur_t==1) {
      s1[i-1]+=SA[s1[i-1]]-1;
    }
    succ_t=cur_t;
  }

  return name_ctr;
}

/*****************************************************************************/

uint_t nameSubstr_generalized(uint_t *SA, 
  uint_t *s, uint_t *s1, uint_t n, 
  uint_t m, uint_t n1, int level, int cs, uint_t separator) {
  uint_t i, j, cur_t, succ_t;

  // init the name array buffer
  for(i=n1; i<n; i++) SA[i]=EMPTY_k;

  // scan to compute the interim s1
  uint_t name=0, name_ctr=0;
  uint_t pre_pos=n-1, pre_len=0;
  for(i=0; i<n1; i++) {
    int diff=false;
    uint_t len, pos=SA[i];

    uint_t d;  
    len=getLengthOfLMS((int_t*)s, n, level, pos, cs);
    if(len!=pre_len) diff=true;
    else
      for(d=0; d<len; d++)
        if(pos+d==n-1 || pre_pos+d==n-1 ||
           chr(pos+d)!=chr(pre_pos+d) ||
           (chr(pos+d)==separator && chr(pre_pos+d)==separator)){
          diff=true; break;
        }

    if(diff) {
      name=i; name_ctr++;
      SA[name]=1; // a new name.
      pre_pos=pos; pre_len=len;
    }
    else
      SA[name]++; // count this name.

    SA[n1+pos/2]=name;
  }

  // compact the interim s1 sparsely stored 
  //   in SA[n1, n-1] into SA[m-n1, m-1].
  for(i=n-1, j=m-1; i>=n1; i--)
    if(SA[i]!=EMPTY_k) SA[j--]=SA[i];

  // rename each S-type character of the
  //   interim s1 as the end of its bucket
  //   to produce the final s1.
  succ_t=1;
  for(i=n1-1; i>0; i--) {
    int_t ch=s1[i], ch1=s1[i-1];
    cur_t=(ch1< ch || (ch1==ch && succ_t==1))?1:0;
    if(cur_t==1) {
      s1[i-1]+=SA[s1[i-1]]-1;
    }
    succ_t=cur_t;
  }
  
  return name_ctr;
}

/*****************************************************************************/

uint_t nameSubstr_generalized_LCP(uint_t *SA, int_t *LCP,
  uint_t *s, uint_t *s1, uint_t n, 
  uint_t m, uint_t n1, int level, int cs, uint_t separator) {
  uint_t i, j, cur_t, succ_t;

  // init the name array buffer
  for(i=n1; i<n; i++) SA[i]=EMPTY_k;

  // scan to compute the interim s1
  uint_t name=0, name_ctr=0;
  uint_t pre_pos=n-1, pre_len=0;
  for(i=0; i<n1; i++) {
    int diff=false;
    uint_t len, pos=SA[i];

    uint_t d;  
    len=getLengthOfLMS((int_t*)s, n, level, pos, cs);
    if(len!=pre_len) diff=true;
    else{
      for(d=0; d<len; d++){
        if(pos+d==n-1 || pre_pos+d==n-1 ||
           chr(pos+d)!=chr(pre_pos+d) ||
           (chr(pos+d)==separator && chr(pre_pos+d)==separator)){
          diff=true; break;
        }
      }
      LCP[i]=d;
    }

    if(diff) {
      name=i; name_ctr++;
      SA[name]=1; // a new name.
      pre_pos=pos; pre_len=len;
    }
    else
      SA[name]++; // count this name.

    SA[n1+pos/2]=name;
  }

  // compact the interim s1 sparsely stored 
  //   in SA[n1, n-1] into SA[m-n1, m-1].
  for(i=n-1, j=m-1; i>=n1; i--)
    if(SA[i]!=EMPTY_k) SA[j--]=SA[i];

  // rename each S-type character of the
  //   interim s1 as the end of its bucket
  //   to produce the final s1.
  succ_t=1;
  for(i=n1-1; i>0; i--) {
    int_t ch=s1[i], ch1=s1[i-1];
    cur_t=(ch1< ch || (ch1==ch && succ_t==1))?1:0;
    if(cur_t==1) {
      s1[i-1]+=SA[s1[i-1]]-1;
    }
    succ_t=cur_t;
  }
  
  return name_ctr;
}

/*****************************************************************************/

void getSAlms(uint_t *SA, 
  int_t *s, 
  uint_t *s1, uint_t n, 
  uint_t n1, int level, int cs) {
  uint_t i, j, cur_t, succ_t;

  j=n1-1; s1[j--]=n-1;
  succ_t=0; // s[n-2] must be L-type
  for(i=n-2; i>0; i--) {
    cur_t=(chr(i-1)<chr(i) ||
          (chr(i-1)==chr(i) && succ_t==1))?1:0;
    if(cur_t==0 && succ_t==1) s1[j--]=i;
    succ_t=cur_t;
  }
}


void getSAlms_DA(uint_t *SA, int_da* DA,
  int_t *s, 
  uint_t *s1, uint_t n, 
  uint_t n1, int level, int cs, uint_t separator) {
  uint_t i, j, cur_t, succ_t;

/**/
  int_t k=0;
  for(i=n-2; i>0; i--) if(chr(i)==separator)k++;
  DA[n1-1]=(int_da) k;
/**/
  
  j=n1-1; s1[j--]=n-1;
  succ_t=0; // s[n-2] must be L-type
  for(i=n-2; i>0; i--) {
	  
	if(chr(i)==separator)k--;

    cur_t=(chr(i-1)<chr(i) ||
          (chr(i-1)==chr(i) && succ_t==1))?1:0;
    if(cur_t==0 && succ_t==1){//LMS-suffix
		s1[j]=i;
		DA[j]=k;
		j--;
	}
    succ_t=cur_t;
  }
  
}


/*****************************************************************************/

int_t SACA_K(int_t	*s, uint_t *SA,
  uint_t n, unsigned int K,
  uint_t m, int cs, int level) {
  uint_t i;
  uint_t *bkt=NULL;
  
  #if PHASES
  time_t t_start_phase = 0.0; 
  clock_t c_start_phase = 0.0;
  #endif

  #if DEPTH
  time_t t_start = time(NULL);
  clock_t c_start =  clock();
  #endif

  #if PHASES
  if(!level){
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  }
  #endif

  // stage 1: reduce the problem by at least 1/2.
  if(level==0) {

    bkt=(uint_t *)malloc(sizeof(int_t)*K);
    putSubstr0(SA, s, bkt, n, K, cs);

    induceSAl0(SA, s, bkt, n, K, false, cs);
    induceSAs0(SA, s, bkt, n, K, false, cs);

  }
  else {
    putSubstr1((int_t *)SA, (int_t *)s,(int_t)n, cs);
    induceSAl1((int_t *)SA, (int_t *)s, n ,false, cs);
    induceSAs1((int_t *)SA, (int_t *)s, n, false, cs);
  }

  // now, all the LMS-substrings are sorted and 
  //   stored sparsely in SA.

  // compact all the sorted substrings into
  //   the first n1 items of SA.
  // 2*n1 must be not larger than n.
  uint_t n1=0;
  for(i=0; i<n; i++) 
    if((!level&&SA[i]>0) || (level&&((int_t *)SA)[i]>0))
      SA[n1++]=SA[i];

  uint_t *SA1=SA, *s1=SA+m-n1;
  uint_t name_ctr;
  name_ctr=nameSubstr(SA,s,s1,n,m,n1,level,cs);

  #if PHASES
  if(!level){
	printf("phase 1:\n");
	time_stop(t_start_phase, c_start_phase);
  }
  #endif

  #if PHASES
  if(!level){
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  }
  #endif

  // stage 2: solve the reduced problem.
  int_t depth=1;
  // recurse if names are not yet unique.
  if(name_ctr<n1)
    depth += SACA_K((int_t*)s1, SA1, 
          n1, 0, m-n1, sizeof(int_t), level+1);
  else // get the suffix array of s1 directly.
    for(i=0; i<n1; i++) SA1[s1[i]]=i;

  // stage 3: induce SA(S) from SA(S1).

  getSAlms(SA, s, s1, n, n1, level, cs);

  for(i=0; i<n1; i++) SA[i]=s1[SA[i]];
  for(i=n1; i<n; i++) SA[i]=level?EMPTY_k:0; 

  if(level==0) {
    putSuffix0(SA, s, bkt, n, K, n1, cs);

    #if PHASES
    printf("phase 2:\n");
    time_stop(t_start_phase, c_start_phase);
    #endif
  
    #if PHASES
    t_start_phase = time(NULL);
    c_start_phase =  clock();
    #endif

    induceSAl0(SA, s, bkt, n, K, true, cs);

    #if PHASES
    printf("phase 3:\n");
    time_stop(t_start_phase, c_start_phase);
    #endif
  
    #if PHASES
    t_start_phase = time(NULL);
    c_start_phase =  clock();
    #endif

    induceSAs0(SA, s, bkt, n, K, true, cs);
    free(bkt);

    #if PHASES
    printf("phase 4:\n");
    time_stop(t_start_phase, c_start_phase);
    #endif
  }
  else {
    putSuffix1((int_t *)SA, (int_t *)s, n1, cs);
    induceSAl1((int_t *)SA, (int_t *)s, n, true, cs);
    induceSAs1((int_t *)SA, (int_t *)s, n, true, cs);
  }

  #if DEPTH
  printf("depth %" PRIdN ":\nname_ctr = %" PRIdN ", n1 =%" PRIdN ", n = %" PRIdN "\n", depth, name_ctr, n1, n);
  time_stop(t_start, c_start);
  #endif

return depth;
}

/*****************************************************************************/

int_t gSACA_K(uint_t *s, uint_t *SA,
  uint_t n, unsigned int K,
  int cs, uint_t separator, int level) {
  uint_t i;
  uint_t *bkt=NULL;
  uint_t m=n;

  #if PHASES
  time_t t_start_phase = 0.0; 
  clock_t c_start_phase = 0.0;
  #endif

  #if DEPTH
  time_t t_start = time(NULL);
  clock_t c_start =  clock();
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif
  // stage 1: reduce the problem by at least 1/2.

  bkt=(uint_t *)malloc(sizeof(int_t)*K);

  putSubstr0_generalized(SA, s, bkt, n, K, cs, separator);

  induceSAl0_generalized(SA, s, bkt, n, K, false, cs, separator);
  induceSAs0_generalized(SA, s, bkt, n, K, false, cs, separator);

  // insert separator suffixes in their buckets
  // bkt[separator]=1; // gsa-is
  for(i=n-3; i>0; i--)
    if(chr(i)==separator)
      SA[bkt[chr(i)]--]=i;

  // now, all the LMS-substrings are sorted and 
  //   stored sparsely in SA.

  // compact all the sorted substrings into
  //   the first n1 items of SA.
  // 2*n1 must be not larger than n.
  uint_t n1=0;
  for(i=0; i<n; i++) 
    if((SA[i]>0))
      SA[n1++]=SA[i];

  uint_t *SA1=SA, *s1=SA+m-n1;
  uint_t name_ctr;
  name_ctr=nameSubstr_generalized(SA,s,s1,n,m,n1,level,cs,separator);

  #if PHASES
	printf("phase 1:\n");
	time_stop(t_start_phase, c_start_phase);
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif

  // stage 2: solve the reduced problem.
  int_t depth=1;
  // recurse if names are not yet unique.
  if(name_ctr<n1)
    depth += SACA_K((int_t*)s1, SA1, 
          n1, 0, m-n1, sizeof(int_t), level+1);
  else // get the suffix array of s1 directly.
    for(i=0; i<n1; i++) SA1[s1[i]]=i;

  // stage 3: induce SA(S) from SA(S1).

  getSAlms(SA, (int_t*)s, s1, n, n1, level, cs);

  for(i=0; i<n1; i++) SA[i]=s1[SA[i]];
  for(i=n1; i<n; i++) SA[i]=0; 

  putSuffix0_generalized(SA, s, bkt, n, K, n1, cs, separator);
  
  #if PHASES
	printf("phase 2:\n");
	time_stop(t_start_phase, c_start_phase);
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif

  induceSAl0_generalized(SA, s, bkt, n, K, true, cs, separator);

  #if PHASES
      printf("phase 3:\n");
      time_stop(t_start_phase, c_start_phase);
  #endif
  
  #if PHASES
      t_start_phase = time(NULL);
      c_start_phase =  clock();
  #endif

  induceSAs0_generalized(SA, s, bkt, n, K, true, cs, separator);

  free(bkt);

  #if DEPTH
  printf("depth %" PRIdN ":\nname_ctr = %" PRIdN ", n1 =%" PRIdN ", n = %" PRIdN "\n", depth, name_ctr, n1, n);
  time_stop(t_start, c_start);
  #endif

  #if PHASES
      printf("phase 4:\n");
      time_stop(t_start_phase, c_start_phase);
  #endif

return depth;
}

/*****************************************************************************/

int_t gSACA_K_LCP(uint_t *s, uint_t *SA, int_t *LCP,
  uint_t n, unsigned int K,
  int cs, uint_t separator, int level) {
  uint_t i;
  uint_t *bkt=NULL;
  uint_t m=n;
  
  #if PHASES
  time_t t_start_phase = 0.0; 
  clock_t c_start_phase = 0.0;
  #endif

  #if DEPTH
  time_t t_start = time(NULL);
  clock_t c_start =  clock();
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif
  // stage 1: reduce the problem by at least 1/2.

  bkt=(uint_t *)malloc(sizeof(int_t)*K);
  putSubstr0_generalized(SA, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("bucket LMS-subs\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  #endif
 
  induceSAl0_generalized(SA, s, bkt, n, K, false, cs, separator);

  #if DEBUG
  printf("L-type\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif

  induceSAs0_generalized(SA, s, bkt, n, K, false, cs, separator);

  #if DEBUG
  printf("S-type\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif
  
  // insert separator suffixes in their buckets
  // bkt[separator]=1; // gsa-is
  for(i=n-3; i>0; i--)
    if(chr(i)==separator)
      SA[bkt[chr(i)]--]=i;

  #if DEBUG
  printf("S-type (separators)\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif

  // now, all the LMS-substrings are sorted and 
  //   stored sparsely in SA.

  // compact all the sorted substrings into
  //   the first n1 items of SA.
  // 2*n1 must be not larger than n.
  uint_t n1=0;
  for(i=0; i<n; i++) 
    if((SA[i]>0))
      SA[n1++]=SA[i];

  uint_t *SA1=SA, *s1=SA+m-n1;
  uint_t name_ctr;

  #if DEBUG
  printf("\nSA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  name_ctr=nameSubstr_generalized_LCP(SA,LCP,s,s1,n,m,n1,level,cs,separator);

  #if DEBUG
  printf("nameSubstr:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  #if PHASES
	printf("phase 1:\n");
	time_stop(t_start_phase, c_start_phase);
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif

  // stage 2: solve the reduced problem.
  int_t depth=1;
  // recurse if names are not yet unique.
  if(name_ctr<n1)
    depth += SACA_K((int_t*)s1, SA1, 
          n1, 0, m-n1, sizeof(int_t), level+1);
  else // get the suffix array of s1 directly.
    for(i=0; i<n1; i++) SA1[s1[i]]=i;

  // stage 3: induce SA(S) from SA(S1).
  #if DEBUG
  printf("recursive:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  getSAlms(SA, (int_t*)s, s1, n, n1, level, cs);

  #if DEBUG
  printf("getSAlms:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  uint_t *RA=s1;
  int_t *PLCP=LCP+m-n1;//PHI is stored in PLCP array

  //compute the LCP of consecutive LMS-suffixes
  compute_lcp_phi_sparse((int_t*)s, SA1, RA, LCP, PLCP, n1, cs, separator); 

  #if DEBUG
  printf("\nPHI-algorithm:\n");
  printf("--\nSA1\n");
  for(i=0; i<n1; i++)//SA1
        printf("%" PRIdN "\t", SA1[i]+1);
  printf("| ");
  for(i=0; i<n1; i++)//s1
        printf("%" PRIdN "\t", RA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n1; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("| ");
  for(i=0; i<n1; i++)
        printf("%" PRIdN "\t", PLCP[i]);
  printf("\n");
  #endif

  for(i=0; i<n1; i++) SA[i]=s1[SA[i]];
  for(i=n1; i<n; i++) SA[i]=U_MAX; 
  for(i=n1;i<n;i++) LCP[i]=0;

  #if DEBUG
  printf("\nstage 3:\n\n");
  printf("mapping back:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

/*
  if(!lcp_array_check((unsigned char*)s, SA1, LCP, n, cs, separator))printf("isNotLCP (lms)!!\n"); 
  else printf("isLCP (lms)!!\n");
*/

  putSuffix0_generalized_LCP(SA, LCP, s, bkt, n, K, n1, cs, separator);
  
  #if PHASES
  if(!level){
	printf("phase 2:\n");
	time_stop(t_start_phase, c_start_phase);
  }
  #endif

  #if PHASES
  if(!level){
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  }
  #endif


  #if DEBUG
  printf("SA (mapped)\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  induceSAl0_generalized_LCP(SA, LCP, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("L-type\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  #if PHASES
  if(!level){
      printf("phase 3:\n");
      time_stop(t_start_phase, c_start_phase);
  }
  #endif
  
  #if PHASES
  if(!level){
      t_start_phase = time(NULL);
      c_start_phase =  clock();
  }
  #endif

  induceSAs0_generalized_LCP(SA, LCP, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("S-type\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif
  free(bkt);

  #if DEPTH
  printf("depth %" PRIdN ":\nname_ctr = %" PRIdN ", n1 =%" PRIdN ", n = %" PRIdN "\n", depth, name_ctr, n1, n);
  time_stop(t_start, c_start);
  #endif

  #if PHASES
  if(!level){
      printf("phase 4:\n");
      time_stop(t_start_phase, c_start_phase);
  }
  #endif

return depth;
}

/*****************************************************************************/

int_t gSACA_K_DA(uint_t *s, uint_t *SA, int_da *DA,
  uint_t n, unsigned int K,
  int cs, uint_t separator, int level) {
  uint_t i;
  uint_t *bkt=NULL;
  uint_t m=n;
  
  #if PHASES
  time_t t_start_phase = 0.0; 
  clock_t c_start_phase = 0.0;
  #endif

  #if DEPTH
  time_t t_start = time(NULL);
  clock_t c_start =  clock();
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif
  // stage 1: reduce the problem by at least 1/2.

  bkt=(uint_t *)malloc(sizeof(int_t)*K);
  putSubstr0_generalized(SA, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("bucket LMS-subs\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  #endif
 
  induceSAl0_generalized(SA, s, bkt, n, K, false, cs, separator);

  #if DEBUG
  printf("L-type\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif

  induceSAs0_generalized(SA, s, bkt, n, K, false, cs, separator);

  #if DEBUG
  printf("S-type\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif
  
  // insert separator suffixes in their buckets
  // bkt[separator]=1; // gsa-is
  for(i=n-3; i>0; i--)
    if(chr(i)==separator)
      SA[bkt[chr(i)]--]=i;

  #if DEBUG
  printf("S-type (separators)\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif

  // now, all the LMS-substrings are sorted and 
  //   stored sparsely in SA.

  // compact all the sorted substrings into
  //   the first n1 items of SA.
  // 2*n1 must be not larger than n.
  uint_t n1=0;
  for(i=0; i<n; i++) 
    if((SA[i]>0))
      SA[n1++]=SA[i];

  uint_t *SA1=SA, *s1=SA+m-n1;
  uint_t name_ctr;

  #if DEBUG
  printf("\nSA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n\n");
  #endif

  name_ctr=nameSubstr_generalized(SA,s,s1,n,m,n1,level,cs,separator);

  #if DEBUG
  printf("nameSubstr:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n\n");
  #endif

  #if PHASES
	printf("phase 1:\n");
	time_stop(t_start_phase, c_start_phase);
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif

  // stage 2: solve the reduced problem.
  int_t depth=1;
  // recurse if names are not yet unique.
  if(name_ctr<n1)
    depth += SACA_K((int_t*)s1, SA1, 
          n1, 0, m-n1, sizeof(int_t), level+1);
  else // get the suffix array of s1 directly.
    for(i=0; i<n1; i++) SA1[s1[i]]=i;

  // stage 3: induce SA(S) from SA(S1).
  #if DEBUG
  printf("recursive:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n\n");
  #endif

  int_da *d1=DA+m-n1;
  
  getSAlms_DA(SA, d1, (int_t*)s, s1, n, n1, level, cs, separator);

  #if DEBUG
  printf("getSAlms:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif

/**/
  for(i=0; i<n1; i++) {DA[i]=d1[SA[i]]; SA[i]=s1[SA[i]];}
  for(i=n1; i<n; i++) {SA[i]=0; DA[i]=0;}
/**/

  #if DEBUG
  printf("\nstage 3:\n\n");
  printf("mapping back:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif

/**/
  putSuffix0_generalized_DA(SA, DA, s, bkt, n, K, n1, cs, separator);

/**/
  
  #if PHASES
  if(!level){
	printf("phase 2:\n");
	time_stop(t_start_phase, c_start_phase);
  }
  #endif

  #if PHASES
  if(!level){
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  }
  #endif


  #if DEBUG
  printf("SA (mapped)\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif

/**/
  induceSAl0_generalized_DA(SA, DA, s, bkt, n, K, cs, separator);
/**/

  #if DEBUG
  printf("L-type\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif

  #if PHASES
  if(!level){
      printf("phase 3:\n");
      time_stop(t_start_phase, c_start_phase);
  }
  #endif
  
  #if PHASES
  if(!level){
      t_start_phase = time(NULL);
      c_start_phase =  clock();
  }
  #endif

/**/
  induceSAs0_generalized_DA(SA, DA, s, bkt, n, K, cs, separator);
/**/

  #if DEBUG
  printf("S-type\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif
  free(bkt);

  #if DEPTH
  printf("depth %" PRIdN ":\nname_ctr = %" PRIdN ", n1 =%" PRIdN ", n = %" PRIdN "\n", depth, name_ctr, n1, n);
  time_stop(t_start, c_start);
  #endif

  #if PHASES
  if(!level){
      printf("phase 4:\n");
      time_stop(t_start_phase, c_start_phase);
  }
  #endif

return depth;
}

/*****************************************************************************/

int_t gSACA_K_LCP_DA(uint_t *s, uint_t *SA, int_t *LCP, int_da *DA,
  uint_t n, unsigned int K,
  int cs, uint_t separator, int level) {
  uint_t i;
  uint_t *bkt=NULL;
  uint_t m=n;
  
  #if PHASES
  time_t t_start_phase = 0.0; 
  clock_t c_start_phase = 0.0;
  #endif

  #if DEPTH
  time_t t_start = time(NULL);
  clock_t c_start =  clock();
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif
  // stage 1: reduce the problem by at least 1/2.

  bkt=(uint_t *)malloc(sizeof(int_t)*K);
  putSubstr0_generalized(SA, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("bucket LMS-subs\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  #endif
 
  induceSAl0_generalized(SA, s, bkt, n, K, false, cs, separator);

  #if DEBUG
  printf("L-type\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif

  induceSAs0_generalized(SA, s, bkt, n, K, false, cs, separator);

  #if DEBUG
  printf("S-type\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif
  
  // insert separator suffixes in their buckets
  // bkt[separator]=1; // gsa-is
  for(i=n-3; i>0; i--)
    if(chr(i)==separator)
      SA[bkt[chr(i)]--]=i;

  #if DEBUG
  printf("S-type (separators)\n");
  for(i=0; i<n; i++)
	if(SA[i]!=0)  printf("%" PRIdN "\t", SA[i]+1);
        else printf("-1\t");
  printf("\n");
  #endif

  // now, all the LMS-substrings are sorted and 
  //   stored sparsely in SA.

  // compact all the sorted substrings into
  //   the first n1 items of SA.
  // 2*n1 must be not larger than n.
  uint_t n1=0;
  for(i=0; i<n; i++) 
    if((SA[i]>0))
      SA[n1++]=SA[i];

  uint_t *SA1=SA, *s1=SA+m-n1;
  uint_t name_ctr;

  #if DEBUG
  printf("\nSA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  name_ctr=nameSubstr_generalized_LCP(SA,LCP,s,s1,n,m,n1,level,cs,separator);

  #if DEBUG
  printf("nameSubstr:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  #if PHASES
	printf("phase 1:\n");
	time_stop(t_start_phase, c_start_phase);
  #endif

  #if PHASES
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  #endif

  // stage 2: solve the reduced problem.
  int_t depth=1;
  // recurse if names are not yet unique.
  if(name_ctr<n1)
    depth += SACA_K((int_t*)s1, SA1, 
          n1, 0, m-n1, sizeof(int_t), level+1);
  else // get the suffix array of s1 directly.
    for(i=0; i<n1; i++) SA1[s1[i]]=i;

  // stage 3: induce SA(S) from SA(S1).
  #if DEBUG
  printf("recursive:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  int_da *d1=DA+m-n1;
  
  getSAlms_DA(SA, d1, (int_t*)s, s1, n, n1, level, cs, separator);


//FELIPE  getSAlms(SA, (int_t*)s, s1, n, n1, level, cs);

  #if DEBUG
  printf("getSAlms:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]); 
  printf("\n\n");
  #endif

  uint_t *RA=s1;
  int_t *PLCP=LCP+m-n1;//PHI is stored in PLCP array

  //compute the LCP of consecutive LMS-suffixes
  compute_lcp_phi_sparse((int_t*)s, SA1, RA, LCP, PLCP, n1, cs, separator); 

  #if DEBUG
  printf("\nPHI-algorithm:\n");
  printf("--\nSA1\n");
  for(i=0; i<n1; i++)//SA1
        printf("%" PRIdN "\t", SA1[i]+1);
  printf("| ");
  for(i=0; i<n1; i++)//s1
        printf("%" PRIdN "\t", RA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n1; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("| ");
  for(i=0; i<n1; i++)
        printf("%" PRIdN "\t", PLCP[i]);
  printf("\n");
  #endif

  for(i=0; i<n1; i++) {DA[i]=d1[SA[i]]; SA[i]=s1[SA[i]];}
  for(i=n1; i<n; i++) {SA[i]=U_MAX; DA[i]=0;}
  for(i=n1;i<n;i++) LCP[i]=0;

//DA  for(i=n1; i<n; i++) {SA[i]=0; DA[i]=-1;}

  #if DEBUG
  printf("\nstage 3:\n\n");
  printf("mapping back:\n");
  printf("SA\n");
  for(i=0; i<n; i++)
    printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif

/*
  if(!lcp_array_check((unsigned char*)s, SA1, LCP, n, cs, separator))printf("isNotLCP (lms)!!\n"); 
  else printf("isLCP (lms)!!\n");
*/

  putSuffix0_generalized_LCP_DA(SA, LCP, DA, s, bkt, n, K, n1, cs, separator);
  
  #if PHASES
  if(!level){
	printf("phase 2:\n");
	time_stop(t_start_phase, c_start_phase);
  }
  #endif

  #if PHASES
  if(!level){
	t_start_phase = time(NULL);
	c_start_phase =  clock();
  }
  #endif


  #if DEBUG
  printf("SA (mapped)\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n\n");
  #endif

  induceSAl0_generalized_LCP_DA(SA, LCP, DA, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("L-type\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif

  #if PHASES
  if(!level){
      printf("phase 3:\n");
      time_stop(t_start_phase, c_start_phase);
  }
  #endif
  
  #if PHASES
  if(!level){
      t_start_phase = time(NULL);
      c_start_phase =  clock();
  }
  #endif

  induceSAs0_generalized_LCP_DA(SA, LCP, DA, s, bkt, n, K, cs, separator);

  #if DEBUG
  printf("S-type\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", SA[i]+1);
  printf("\n");
  printf("LCP\n");
  for(i=0; i<n; i++)
        printf("%" PRIdN "\t", LCP[i]);
  printf("\n");
  printf("DA\n");
  for(i=0; i<n; i++)
        printf("%" PRIdA "\t", DA[i]);
  printf("\n\n");
  #endif
  free(bkt);

  #if DEPTH
  printf("depth %" PRIdN ":\nname_ctr = %" PRIdN ", n1 =%" PRIdN ", n = %" PRIdN "\n", depth, name_ctr, n1, n);
  time_stop(t_start, c_start);
  #endif

  #if PHASES
  if(!level){
      printf("phase 4:\n");
      time_stop(t_start_phase, c_start_phase);
  }
  #endif

return depth;
}

/*****************************************************************************/


int sacak(unsigned char *s, uint_t *SA, uint_t n){
	if((s == NULL) || (SA == NULL) || (n < 0)) return -1;
	return SACA_K((int_t*)s, (uint_t*)SA, n, 256, n, sizeof(char), 0);
}

int sacak_int(int_text *s, uint_t *SA, uint_t n, uint_t k){
	if((s == NULL) || (SA == NULL) || (n < 0)) return -1;
	return SACA_K((int_t*)s, (uint_t*)SA, n, k, n, sizeof(int_text), 0);
}

int gsacak(unsigned char *s, uint_t *SA, int_t *LCP, int_da *DA, uint_t n){

	if((s == NULL) || (SA == NULL) || (n < 0)) return -1;

	#if EMPTY_STRING
		for(i=0; i<n-1; i++) if(s[i]==1 && s[i+1]==1) return -2; 
	#endif  

	if((LCP == NULL) && (DA == NULL))
		return gSACA_K((uint_t*)s, SA, n, 256, sizeof(char), 1, 0);
	else if (DA == NULL)
		return gSACA_K_LCP((uint_t*)s, SA, LCP, n, 256, sizeof(char), 1, 0);
	else if (LCP == NULL)
		return gSACA_K_DA((uint_t*)s, SA, DA, n, 256, sizeof(char), 1, 0);
	else
		return gSACA_K_LCP_DA((uint_t*)s, SA, LCP, DA, n, 256, sizeof(char), 1, 0);
}

int gsacak_int(int_text *s, uint_t *SA, int_t *LCP, int_da *DA, uint_t n, uint_t k){

	if((s == NULL) || (SA == NULL) || (n < 0)) return -1;

	if((LCP == NULL) && (DA == NULL))
		return gSACA_K((uint_t*)s, SA, n, k, sizeof(int_text), 1, 0);
	else if (DA == NULL)
		return gSACA_K_LCP((uint_t*)s, SA, LCP, n, k, sizeof(int_text), 1, 0);
	else if (LCP == NULL)
		return gSACA_K_DA((uint_t*)s, SA, DA, n, k, sizeof(int_text), 1, 0);
	else
		return gSACA_K_LCP_DA((uint_t*)s, SA, LCP, DA, n, k, sizeof(int_text), 1, 0);
}

/*****************************************************************************/
