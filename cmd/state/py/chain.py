import dbutils
import common

import rlp as rlp
from eth.rlp.headers import BlockHeader, BlockHeaderAPI


def lastBlockNumber(env):
    b = env.open_db(dbutils.HeadHeaderKey, create=False)
    b1 = env.open_db(dbutils.HeaderNumberPrefix, create=False)
    with env.begin(write=False) as txn:
        blockHashData = txn.get(dbutils.HeadHeaderKey, db=b)
        assert len(blockHashData) == common.HashLength, "%d != %d" % (len(blockHashData), common.HashLength)
        blockNumberData = txn.get(blockHashData, db=b1)
        assert len(blockNumberData) == 8
        return common.bytesToUint64(blockNumberData)


def decode_block_header(header_rlp: bytes) -> BlockHeaderAPI:
    return rlp.decode(header_rlp, sedes=BlockHeader)
