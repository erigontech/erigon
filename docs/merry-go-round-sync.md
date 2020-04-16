# Merry-Go-Round sync (turbo-geth design)

Initial discussion here: https://ethresear.ch/t/merry-go-round-sync/7158

## Participants

We divide participants into two types: seeders and leechers. Seeders are the ones who have the full copy of the current Ethereum
state, and they also keep updating it as new blocks appear. Leechers are the ones that join the Merry-Go-Round network to
either acquire the full copy of the state, or "sift" through it to acquire some part of the state (this is what some "Stateless"
node might like to do in the beginning).

## Syncing cycles

The process of syncing happens in cycles. During each cycle, all participants exchange information about certain piece of state.


## How will seeders produce sync schedule

