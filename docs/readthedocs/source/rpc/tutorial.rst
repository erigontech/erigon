Tutorial: Build a personalized daemon
=====================================

For the following tutorial, we will make references to https://github.com/Giulio2002/hello-tg-daemon.

We are going to build our daemon using golang and Erigon packages, so first of all we are going to create a file in which we are going to store our API methods and informations. (`api.go`).

our daemon will only contain one method: `myNamespace_getBlockNumberByHash` which will return the block number associated to certain hash.

.. code-block:: go

    package main

    import (
        "context"

        "github.com/ledgerwatch/erigon-lib/kv"
        "github.com/ledgerwatch/erigon/common"
        "github.com/ledgerwatch/erigon/core/rawdb"
    )

    type API struct {
        db kv.RoDB
    }

    type ExampleAPI interface {
        GetBlockNumberByHash(ctx context.Context, hash common.Hash) (uint64, error)
    }

    func NewAPI(db kv.RoDB) *API {
        return &API{db}
    }

    func (api *API) GetBlockNumberByHash(ctx context.Context, hash common.Hash) (uint64, error) {
        tx, err := api.db.BeginRo(ctx)
        if err != nil {
            return 0, err
        }
        defer tx.Rollback()

        block, err := rawdb.ReadBlockByHash(tx, hash)
        if err != nil {
            return 0, err
        }
        return block.NumberU64(), nil
    }

The type `Api` is the type that is going to contain the methods for our custom daemon. This type has one member: `db` object used to interact with the Erigon node remotely. Member `db` behave like normal db object and can be used alongside with the rawdb package.

In our example we are making an rpcdaemon call that by receiving a certain block hash, it give the block number associated as an output. this is all done in `GetBlockNumberByHash`.

Now we are going to make our `main.go` where we are going to serve the api we made in `api.go`.

.. code-block:: go

    package main

    import (
        "os"

        "github.com/ledgerwatch/erigon-lib/kv"
        "github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
        "github.com/ledgerwatch/erigon/cmd/utils"
        "github.com/ledgerwatch/erigon/rpc"
        "github.com/ledgerwatch/log/v3"
        "github.com/spf13/cobra"
    )

    func main() {
        cmd, cfg := cli.RootCommand()
        rootCtx, rootCancel := utils.RootContext()
        cmd.RunE = func(cmd *cobra.Command, args []string) error {
            logger := log.New()
            db, _, _, _, err := cli.RemoteServices(*cfg, logger, rootCancel)
            if err != nil {
                log.Error("Could not connect to DB", "error", err)
                return nil
            }
            defer db.Close()

            if err := cli.StartRpcServer(cmd.Context(), *cfg, APIList(db)); err != nil {
                log.Error(err.Error())
                return nil
            }
            return nil
        }
        if err := cmd.ExecuteContext(rootCtx); err != nil {
            log.Error(err.Error())
            os.Exit(1)
        }
    }

    func APIList(db kv.RoDB) []rpc.API {
        api := NewAPI(db)
        customAPIList := []rpc.API{
            {
                Namespace: "myNamespace",
                Public:    true,
                Service:   ExampleAPI(api),
                Version:   "1.0",
            },
        }
        return customAPIList
    }

In the main we are just running our rpcdaemon as we defined it in `APIList`, in fact in `APIList` we are configuring our custom rpcdaemon to serve the ExampleAPI's methods on namespace `myNamespace` meaning that in order to call GetBlockNumberByHash via json rpc we have to call method `myNamespace_getBlockNumberByHash`.

Let's now try it:

.. code-block:: sh

    $ go build
    $ ./hello-erigon-daemon --http.api=myNamespace # the flag enables our namespace.

**Note: Remember to run it with --private.api.addr=localhost:9090 and/or --datadir <path-to-erigon-data>**

now it should be all set and we can test it with:

.. code-block:: sh

    curl -H "Content-Type: application/json" -X POST --data '{"jsonrpc":"2.0","method":"myNamespace_getBlockNumberByHash","params":["ANYHASH"],"id":1}' localhost:8545

another example of custom daemon can be found at https://github.com/erigontech/project-1/blob/master/api.go.

Happy Building ~~~.
