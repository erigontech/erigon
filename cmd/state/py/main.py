import csv
import struct

import plotly.express as px
import pandas as pd
import lmdb
import sys

import chain
import dbutils
import common

# apt install python3-snappy libgmp3-dev && pip3 install trinity lmdb pandas plotly

cmd = sys.argv[1]
chaindata = sys.argv[2]

env = lmdb.open(chaindata, max_dbs=100, readonly=True, subdir=True, map_size=32 * 1024 * 1024 * 1024, create=False)
analyticsEnv = lmdb.open("analytics", max_dbs=100, readonly=False, subdir=True, map_size=32 * 1024 * 1024 * 1024,
                         create=True)

env.reader_check()  # clear stale reads

def allBuckets(env):
    buckets = []
    root = env.open_db(None, create=False)
    with env.begin(write=False) as txn:
        with txn.cursor(root) as curs:
            for i, (k, v) in enumerate(curs.iternext()):
                buckets.append(k.decode("utf-8"))
    return buckets

if cmd == "stats":
    data = {"name": [], "size": []}
    for bucket in allBuckets(env):
        b = env.open_db(bucket.encode(), create=False)
        with env.begin(write=False) as txn:
            stat = txn.stat(b)
            size = stat['psize'] * (stat['branch_pages'] + stat['leaf_pages'] + stat['overflow_pages'])
            print("%s: %dMb %dM" % (bucket, size/1024/1024, stat['entries']/1000/1000))
#             data["name"].append(bucket)
#             data["size"].append(stat['psize'] * (stat['branch_pages'] + stat['leaf_pages'] + stat['overflow_pages']))
#     df = pd.DataFrame.from_dict(data)
#     fig = px.pie(df, values='size', names='name', title='Buckets size')
#     fig.show()
elif cmd == "gas_limits":
    StartedWhenBlockNumber = chain.lastBlockNumber(env)

    b = env.open_db(dbutils.HeaderPrefix, create=False)
    mainHashes = analyticsEnv.open_db("gl_main_hashes".encode(), create=True)


    def collect_main_hashes(readTx, writeTx):
        with readTx.cursor(b) as curs:
            for i, (k, v) in enumerate(curs.iternext()):
                timestamp = common.bytesToUint64(k[:common.BlockNumberLength])
                if timestamp > StartedWhenBlockNumber:
                    break
                if not dbutils.isHeaderHashKey(k):
                    continue

                mainHash = bytes(v)
                writeTx.put(mainHash, common.uint64ToBytes(0), mainHashes)


    def gas_limits(readTx, writeTx, file):
        blockNum = 0
        with readTx.cursor(b) as curs:
            for i, (k, v) in enumerate(curs.iternext()):
                timestamp = common.bytesToUint64(k[:common.BlockNumberLength])
                if timestamp > StartedWhenBlockNumber:
                    break
                if not dbutils.isHeaderKey(k):
                    continue
                val = writeTx.get(k[common.BlockNumberLength:], None, mainHashes)
                if val is None:
                    continue
                header = chain.decode_block_header(v)
                file.writerow([blockNum, header.GasLimit])
                blockNum += 1


    with env.begin(write=False) as txn:
        with analyticsEnv.begin(write=True) as writeTx:
            with open('gas_limits.csv', 'w') as csvfile:
                collect_main_hashes(txn, writeTx)
                print("Preloaded: %d" % writeTx.stat(mainHashes)["entries"])
                gas_limits(txn, writeTx, csv.writer(csvfile))

else:
    print("unknown command %s" % cmd)
