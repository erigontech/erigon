import pandas as pd
from web3 import Web3

# Ethereum node endpoint (QuikNode)
RPC_URL = "https://warmhearted-smart-sanctuary.quiknode.pro/083185936fbe70859f03dcd17c7ef919182a2071/"
# Target recipient address (normalized to lowercase)
TARGET_ADDRESSES = ["0x0a252663DBCC0B073063D6420A40319E438CFA59".lower(), "0x2F848984984D6C3c036174ce627703Edaf780479".lower(), "0x3fC29836E84E471a053D2D9E80494A867D670EAD".lower()]

# Input and output files
def main(input_file: str, chunksize: int = 1000):
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        raise ConnectionError(f"Could not connect to Ethereum node at {RPC_URL}")

    totalNotZen = 0
    # Prepare CSV streaming
    first_chunk = True
    for chunk in pd.read_csv(input_file, usecols=["tx_hash"], chunksize=chunksize):
        # Collect flags for this chunk
        #flags = []
        for tx_hash in chunk["tx_hash"]:
            try:
                tx = w3.eth.get_transaction(tx_hash)
                # Normalize recipient
                recipient = tx.to.lower() if tx.to else None
                if recipient not in TARGET_ADDRESSES:
                    totalNotZen += 1
                    #flags.append(True)
                    print(f"tx is not XEN: Processing tx {tx_hash}: recipient {recipient}")
                
            except Exception as e:
                # On error (invalid hash, missing tx), mark False
                print(f"Error fetching tx {tx_hash}: {e}")
        print(f"Total transactions not meant for XEN minting: {totalNotZen}")
        # # Append result column
        # chunk["is_target_recipient"] = flags

        # # Write or append to output
        # if first_chunk:
        #     chunk.to_csv(output_file, index=False, mode="w")
        #     first_chunk = False
        # else:
        #     chunk.to_csv(output_file, index=False, header=False, mode="a")

    #print(f"Processing complete. Results written to {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check if transactions go to a specific recipient.")
    parser.add_argument("input_csv", help="Path to input CSV containing tx_hash column.")
    parser.add_argument("--chunksize", type=int, default=1000, help="Number of rows per batch read.")
    args = parser.parse_args()

    main(args.input_csv, args.chunksize)
