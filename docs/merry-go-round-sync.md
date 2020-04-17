# Merry-Go-Round sync (turbo-geth design)

Initial discussion here: https://ethresear.ch/t/merry-go-round-sync/7158

## Participants

We divide participants into two types: seeders and leechers. Seeders are the ones who have the full copy of the current Ethereum
state, and they also keep updating it as new blocks appear. Leechers are the ones that join the Merry-Go-Round network to
either acquire the full copy of the state, or "sift" through it to acquire some part of the state (this is what some "Stateless"
node might like to do in the beginning).

## Syncing cycles

The process of syncing happens in cycles. During each cycle, all participants attempt to distribute the entire content of the
Ethereum state to each other. Therefore the duration of one cycle for the Ethereum mainnet is likely to be some hours.
Cycle is divided into ticks. For convinience, we can say that each ticks starts after the mining of certain block, and lasts
for some predetermined amount of time, let's say, 30 seconds. This definition means that the ticks will often overlap, but not
always, as shown onn the picture below.

![cycles-and-ticks](mgr-sync-1.png)



## How will seeders produce sync schedule

