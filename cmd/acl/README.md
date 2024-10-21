# ACL - tool for managing access lists

In the root of `Erigon` project, use this command to build the commands:

```shell
    make acl
```

It can then be run using the following command

```shell
    ./buid/bin/acl sub-command options...
```

Snapshots supports the following sub commands:

## data-dir

Examples on how to setup your data-dir

```shell
    # on sequencer:
    /Path/Choosen/OnHermezConfig/txpool
    # example
    /Users/{$USER}/code/erigon-data/chain/txpool
    # on default path:
    /Users/{$USER}/Library/Erigon
```

## mode - set mode of access list 

This command takes the following form: 

```shell
    acl mode --datadir=<data-dir> --mode=<mode> 
```

## supported ACL Types
- `allowlist` - allow list type
- `blocklist` - block list type
- `disabled` - doesn't block or allow, everyone is able to do transactions and deploy contracts.

## supported policies
- `sendTx` - enables or disables ability of an account to send transactions (deploy contracts transactions not included).
- `deploy` - enables or disables ability of an account to deploy smart contracts (other transactions not included)

This command updates the `mode` of access list in the `acl` data base. Supported modes are:
- `disabled` - access lists are disabled.
- `allowlist` - allow list is enabled. If address is not in the allow list, it won't be able to send transactions (regular, contract deployment, or both).
- `blocklist` - block list is enabled. If address is in the block list, it won't be able to send transactions (regular, contract deployment, or both).

## update - update access list

This command can be used to update an access list in the `acl` data base.

This command takes the following form: 

```shell
    acl update --datadir=<data-dir> --type=<type> --csv=<path_to_csv>
```
The `update` command will read the `.csv` file provided which should be in format `address,"policy1,policy2"`, and update the defined `acl` in the `db`. Note that the `.csv` file is considered as the final state of policies for given `acl` type for defined addresses, meaning, if an address in the `.csv` file has `sendTx` policy, but in `db` it had `deploy`, after this command, it will have `sendTx` in the `db`, there is no appending. Also, it is worth mentioning that using a `.csv` file user can delete addresses from an `acl` table by leaving policies string as empty `""`. This will tell the command that the user wants to remove an address completely from an `acl`.

## add - adds a policy to an account

This command can be used to add a policy to an account in the specified `acl`.

This command takes the following form: 

```shell
    acl add --datadir=<data-dir> --type=<type> --address=<address> --policy=<policy>
```

The `add` command will add the given policy to an account in given access list table if account is not already added to access list table, or if given account does not have that policy.

## remove - removes a policy from an account

This command can be used to remove a policy from an account in the specified `acl`.

This command takes the following form: 

```shell
    acl remove --datadir=<data-dir> --type=<type> --adress=<address> --policy=<policy>
```
The `remove` command will remove the given policy from an account in given access list table if given account has that policy assigned.

## list - log the information in current acl data-dir

```shell
    acl list --datadir=<data-dir> --log_count=<number_integer>[optional]
```

## operating example:

```shell
    acl list  --datadir=/Users/username_pc_mac/path_to_data/erigon-data/devnet/txpool

    acl add --address=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --policy=deploy --type=blocklist --datadir=/Users/username_pc_mac/path_to_data/erigon-data/devnet/txpool
    acl add --address=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --policy=sendTx --type=blocklist --datadir=/Users/username_pc_mac/path_to_data/erigon-data/devnet/txpool

    acl add --address=0x0921598333Cf3cE5FE2031C056C79aec59EE10b6 --policy=sendTx --type=allowlist --datadir=/Users/username_pc_mac/path_to_data/erigon-data/devnet/txpool
    acl remove --address=0x0921598333Cf3cE5FE2031C056C79aec59EE10b6 --policy=sendTx --type=allowlist --datadir=/Users/username_pc_mac/path_to_data/erigon-data/devnet/txpool

    acl mode --mode=disabled --datadir=/Users/username_pc_mac/path_to_data/erigon-data/devnet/txpool --log_count=20
```