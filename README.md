# erigon-lib
Dependencies of Erigon project, rewritten from scratch and licensed under Apache 2.0

## Dev workflow

In erigon folder create go.work file (it’s already in .gitignore)
```
go 1.20

use (
    .

     ./../erigon-lib
)
```

Create PR in erigon-lib, don’t merge PR, refer from erigon to non-merged erigon-lib branch (commit) by: 
go get github.com/ledgerwatch/erigon-lib/kv@<commit_hash> 

Create Erigon PR

When both CI are green - merge 2 PR. That’s it.
