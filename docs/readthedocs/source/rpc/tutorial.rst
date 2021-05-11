Tutorial: Build a personalized daemon
=====================================

For the following tutorial, we will make references to https://github.com/Giulio2002/hello-tg-daemon.

We are going to build our daemon using golang and turbo-geth packages, so first of all we are going to create a file in which we are going to store our API methods and informations. (`api.go`).

our daemon will only contain one method: `myNamespace_getBlockNumberByHash` which will return the block number associated to certain hash.

.. code-block:: go

    package main

    import (
        "context"

        "github.com/ledgerwatch/turbo-geth/common"
        "github.com/ledgerwatch/turbo-geth/core/rawdb"
        "github.com/ledgerwatch/turbo-geth/ethdb"
    )

    // API - implementation of ExampleApi
    type API struct {
        kv ethdb.RwKV
        db ethdb.Getter
    }

    type ExampleAPI interface {
        GetBlockNumberByHash(ctx context.Context, hash common.Hash) (uint64, error)
    }

    func NewAPI(kv ethdb.RwKV, db ethdb.Getter) *API {
        return &API{kv: kv, db: db}
    }

    func (api *API) GetBlockNumberByHash(ctx context.Context, hash common.Hash) (uint64, error) {
        return rawdb.ReadBlockByHash(api.db, hash).NumberU64(), nil
    }

The type `Api` is the type that is going to contain the methods for our custom daemon. This type has two members: `kv` and `db` which are objects used to interact with the turbo-geth node remotely. they behave like normal db objects and can be used alongside with the rawdb package.

In our example we are making an rpcdaemon call that by receiving a certain block hash, it give the block number associated as an output. this is all done in `GetBlockNumberByHash`.

Now we are going to make our `main.go` where we are going to serve the api we made in `api.go`.

.. code-block:: go

    package main

    import (
        "context"
        "os"

        "github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
        "github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
        "github.com/ledgerwatch/turbo-geth/cmd/utils"
        "github.com/ledgerwatch/turbo-geth/common"
        "github.com/ledgerwatch/turbo-geth/ethdb"
        "github.com/ledgerwatch/turbo-geth/log"
        "github.com/ledgerwatch/turbo-geth/rpc"
        "github.com/spf13/cobra"
    )

    func main() {
        cmd, cfg := cli.RootCommand()
        cmd.RunE = func(cmd *cobra.Command, args []string) error {
            db, backend, err := cli.OpenDB(*cfg)
            if err != nil {
                log.Error("Could not connect to remoteDb", "error", err)
                return nil
            }

            apiList := APIList(db, backend, cfg)
            return cli.StartRpcServer(cmd.Context(), *cfg, apiList)
        }

        if err := cmd.ExecuteContext(utils.RootContext()); err != nil {
            log.Error(err.Error())
            os.Exit(1)
        }
    }

    func APIList(kv ethdb.RwKV, eth ethdb.Backend, cfg *cli.Flags) []rpc.API {
        dbReader := ethdb.NewObjectDatabase(kv)
        api := NewAPI(kv, dbReader)

        customAPIList := []rpc.API{
            {
                Namespace: "myNamespace",
                Public:    true,
                Service:   ExampleAPI(api),
                Version:   "1.0",
            },
        }

        // Add default TurboGeth api's
        return commands.APIList(kv, eth, *cfg, customAPIList)
    }

In the main we are just running our rpcdaemon as we defined it in `APIList`, in fact in `APIList` we are configuring our custom rpcdaemon to serve the ExampleAPI's mathods on namespace `myNamespace` meaning that in order to call GetBlockNumberByHash via json rpc we have to call method `myNamespace_getBlockNumberByHash`.

Let's now try it:

.. code-block:: sh

    $ go build
    $ ./hello-tg-daemon --http.api=myNamespace # the flag enables our namespace.

**Note: Remember to run turbo-geth with --private.api.addr=localhost:9090**

now it should be all set and we can test it with:

.. code-block:: sh

    curl -H "Content-Type: application/json" -X POST --data '{"jsonrpc":"2.0","method":"myNamespace_getBlockNumberByHash","params":["ANYHASH"],"id":1}' localhost:8545

another example of custom daemon can be found at https://github.com/torquem-ch/project-1/blob/master/api.go.

Happy Building ~~~.