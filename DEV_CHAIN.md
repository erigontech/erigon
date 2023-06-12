Steps to setup and run Erigon dev chain. This tutorial is made for macOS.


## 1. Clone and Build Erigon 
Open terminal 1 and type the following command

```bash 
git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git
cd erigon
make erigon
```


## 2. Build RPC daemon
On the same terminal folder you can build the RPC daemon.

```bash
make rpcdaemon
```

## 3. Start Node 1 
If everything is fine, by changing directory to erigon/build/bin you will see the two exec for erigon and rpc daemon.
On the terminal you can type the following command to start node1.

```bash
./erigon --datadir=dev --chain=dev --private.api.addr=localhost:9090 --mine
```
 
 Argument notes:
 * datadir : Tells where the data is stored, default level is dev folder.
 * chain : Tells that we want to run Erigon in the dev chain.
 * private.api.addr=localhost:9090 : Tells where Eigon is going to listen for connections.
 * mine : Add this if you want the node to mine.
 * dev.period <number-of-seconds>: Add this to specify the timing interval among blocks. Number of seconds MUST be > 0 (if you want empty blocks) otherwise the default value 0 does not allow mining of empty blocks.
 
The result will be something like this:

<img width="1652" alt="Node 1 start" src="https://user-images.githubusercontent.com/24697803/140478108-c93a131d-745d-45ac-a76f-9bb808e504df.png">

    
Now save the enode information generated in the logs, we will use this in a minute. Here there is an example.
```   
enode://d30d079163d7b69fcb261c0538c0c3faba4fb4429652970e60fa25deb02a789b4811e98b468726ba0be63b9dc925a019f433177eb6b45c23bb78892f786d8f7a@127.0.0.1:53171 
```

## 4. Start RPC daemon

Open terminal 2 and navigate to erigon/build/bin folder. Here type the following command
    
```bash 
./rpcdaemon --datadir=dev  --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool,parity
```
The result will look like this:
<img width="1636" alt="rpc daemon start" src="https://user-images.githubusercontent.com/24697803/140478408-ac1be94a-4a63-42c6-8673-e24decadd658.png">

    
    
## 5. Start Node 2
    
Node 2 has to connect to Node 1 in order to sync. As such, we will use the argument --staticpeers.
To tell Node 2 where Node 1 is we will use the Enode info of Node 1 we saved before.

Open terminal 3 and navigate to erigon/build/bin folder. Paste in the following command the Enode info and run it, be careful to remove the last part ?discport=0.

```bash  
./erigon --datadir=dev2  --chain=dev --private.api.addr=localhost:9091 \
    --staticpeers="enode://d30d079163d7b69fcb261c0538c0c3faba4fb4429652970e60fa25deb02a789b4811e98b468726ba0be63b9dc925a019f433177eb6b45c23bb78892f786d8f7a@127.0.0.1:53171" \
    --nodiscover
```
    
To check if the nodes are connected, you can go to the log of both nodes and look for the line
    
  ```  [p2p] GoodPeers    eth66=1 ```
    
Note: this might take a while it is not instantaneous, also if you see a 1 on either one of the two the node is fine.
    
    
 
    
    
## 6. Interact with the node using RPC 
    
  Open a terminal 4 and type 
    
```bash
  curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' localhost:8545

```
  The result should look like this: 
    
```json
{"jsonrpc":"2.0","id":1,"result":"0x539"}
```

Other commands you can try:

 ```bash
 curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id":1}' localhost:8545
 ```
 ```bash
 curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_mining", "params": [], "id":1}' localhost:8545
 ```
 ```bash
  curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id":1}' localhost:8545
 ```
 ```bash
  curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "net_peerCount", "params": [], "id":74}' localhost:8545
 ```
 ```bash
  curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":83}' localhost:8545
 ```
    
## 7. Send a transaction with MetaMask
    
 Finally, we want to try sending a transaction between two accounts.
 For this example we will use dev accounts retrieved from Erigon code:
 
 * Account with balance (Dev 1) 
   * address = ``` 0x67b1d87101671b127f5f8714789C7192f7ad340e ``` 
   * privateKey = ``` 26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48 ```
 
 * Empty account (Dev 2)
   * address = ``` 0xa94f5374Fce5edBC8E2a8697C15331677e6EbF0B ``` 
   * privateKey = ``` 45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8 ```
 
 

Now from MetaMask, you can import Dev 1 , and then send a transaction to pass some ethers from Dev 1 to Dev 2. 
From the RPC daemon terminal, you will see something like this
 
<img width="1633" alt="Transaction example" src="https://user-images.githubusercontent.com/24697803/140479146-94b6e66c-22b7-4d8a-a160-b3643d27b612.png">

 Finally you will see the ethers in the Dev 2 account balance.
    


 ## 7. Check a mined block
 
 Now we want to check the creation of a new block and that all the nodes sync.

Below we can see that block 1 is created (blocn_num=1) and that the next block to be proposed increments from 1 to 2 ( block=2). The other nodes will see the same update.
 
<img width="1327" alt="Block" src="https://user-images.githubusercontent.com/24697803/140509913-b2fc3140-ad81-4bf3-a595-d102f7c75245.png">
 
