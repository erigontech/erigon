#!/bin/bash
set -e

dir=$1
files=$(ls $dir/snapshots | grep -v old | grep -v tor | grep seg |  sort -n -t$'-' -k1)
for file in $files
do
   from=$dir/snapshots/$file
   to=$dir/snapshots/$file.new
   echo "file: $file"

   ./build/bin/erigon_old snapshots uncompress $from | ./build/bin/erigon seg compress $to --datadir=$dir
   a=$(du -h $from | awk '{print $1;}')
   b=$(du -h $to | awk '{print $1;}')
   echo "size: $a -> $b"
   mv $from $from.old
   mv $from.new $from
   
done




./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-receiptcache.1472-1536.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=8000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc8k-receiptcache.1472-1536.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-receiptcache.1472-1536.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=5 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=8000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min5-max128-s1-sc8k-receiptcache.1472-1536.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-receiptcache.1472-1536.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=5 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min5-max128-s1-sc4k-receiptcache.1472-1536.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-receiptcache.1472-1536.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc4k-receiptcache.1472-1536.v --datadir=/erigon-data --log.console.verbosity=5

./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-receiptcache.1472-1536.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=4 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s4-sc4k-receiptcache.1472-1536.v --datadir=/erigon-data --log.console.verbosity=5

./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.0-64.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc4k-rcache.0-64.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.0-64.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=4 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s4-sc4k-rcache.0-64.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.0-64.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=8000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc8k-rcache.0-64.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.0-64.v | SnappyEachWord=true DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=8000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc8k-snappy-rcache.0-64.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.0-64.v | SnappyEachWord=true DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc4k-snappy-rcache.0-64.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.384-448.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc4k-snappyfirst-rcache.384-448.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.384-448.v | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft2m-hard128k-min20-max128-s1-sc4k-snappyfirst-rcache.384-448.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.384-448.v | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=2000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft2m-hard128k-min20-max128-s1-sc2k-snappyfirst-rcache.384-448.v --datadir=/erigon-data --log.console.verbosity=5





./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.0-64.v | UnSnappyEachWord=true ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc4k-snappy-rcache.0-64.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-2.v | UnSnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-nosnappy-commitment.0-2.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-2.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-snappy-commitment.0-2.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.8-12.v | UnSnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-nosnappy-commitment.8-12.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.8-12.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-snappy-commitment.8-12.v --datadir=/erigon-data --log.console.verbosity=5



./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-4.v | UnSnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-nosnappy-commitment.0-4.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v99-snappy-commitment.0-8.v | UnSnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-nosnappy-commitment.0-8.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-8.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-zstd-commitment.0-8.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-4.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=4000 ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc4k-snappyfirst-commitment.0-4.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-4.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=2000 ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc2k-snappyfirst-commitment.0-4.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-4.v | DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=1000 ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc1k-snappyfirst-commitment.0-4.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.0-4.v | UnSnappyEachWord=true DictReducerSoftLimit=1_000_000 MaxDictPatterns=64_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=1000 ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-soft1m-hard64k-min20-max128-s1-sc1k-nosnappy-commitment.0-4.v --datadir=/erigon-data --log.console.verbosity=5

./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-accounts.3264-3328.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-snappyfirst-accounts.3264-3328.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-accounts.3264-3328.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-snappyfirst-accounts.3264-3328.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-accounts.3264-3328.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-snappyfirst-accounts.3264-3328.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-accounts.3264-3328.v | SnappyEachWord=true NoCompress=true ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-snappyfirst-accounts.3264-3328.v --datadir=/erigon-data --log.console.verbosity=5


./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.384-448.v | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=1024 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft2m-hard128k-min20-max128-s1-sc1k-snappyfirst-rcache.384-448.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/history/v1-rcache.384-448.v | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=5 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=2000 ./build/bin/erigon seg compress /erigon-data/snapshots/history/v99-soft2m-hard128k-min20-max128-s1-sc2k-snappyfirst-rcache.384-448.v --datadir=/erigon-data --log.console.verbosity=5




./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.8-12.v | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=2000 ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-soft2m-hard128k-min20-max128-s1-sc2k-nosnappy-commitment.8-12.v --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/sepolia_remove_me3/snapshots/history/v1-commitment.8-12.v | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=2000 ./build/bin/erigon seg compress /erigon-data/sepolia_remove_me3/snapshots/history/v99-soft2m-hard128k-min20-max128-s1-sc2k-supst32m-commitment.8-12.v --datadir=/erigon-data --log.console.verbosity=5



./build/bin/erigon snapshots uncompress /erigon-data/snapshots/v1-070300-070400-transactions.seg | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=20 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=1024 ./build/bin/erigon seg compress  /erigon-data/snapshots/v99-soft2m-hard128k-min20-max128-s1-sc1k-supst32m-070300-070400-transactions.seg --datadir=/erigon-data --log.console.verbosity=5
./build/bin/erigon snapshots uncompress /erigon-data/snapshots/v1-070300-070400-transactions.seg | DictReducerSoftLimit=2_000_000 MaxDictPatterns=128_000 MinPatternLen=5 MaxPatternLen=128 SamplingFactor=1 MinPatternScore=1024 ./build/bin/erigon seg compress  /erigon-data/snapshots/v99-soft2m-hard128k-min5-max128-s1-sc1k-supst32m-070300-070400-transactions.seg --datadir=/erigon-data --log.console.verbosity=5










