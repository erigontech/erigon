# Shutter

## What is it?

The Shutter Network provides a solution for an encrypted transaction pool using threshold encryption. Encrypted
transaction pools can protect users from malicious MEV attacks such as front-running and sandwich attacks. You can read
more about Shutter Network at their official website https://www.shutter.network.

## How it works?

There are three main parts that are needed to have a Shutter encrypted transaction pool up and running:

- Keypers
- Encrypting RPC Servers
- Shutterized Validators

Currently, the Shutter encrypted transaction pool is available on Gnosis with aspirations of this one day becoming
available on Ethereum too.

As a result, Erigon has committed to adding support for running as a Shutterized Validator on Gnosis.

This means that you can now run Erigon as a validator which can build blocks that can include transactions from the
Shutter encrypted transaction pool using just-in-time decryption.

The official specs for how the Shutter encrypted transaction pool works can be found
at https://github.com/gnosischain/specs/tree/master/shutter.

## How to run it?

1. Setup your validators and deposit your stake by following https://docs.gnosischain.com/node/manual/validator/deposit
2. Register you validators as "Shutterized Validators" by following the steps
   in https://github.com/NethermindEth/shutter-validator-registration
3. You can run
   `erigon shutter-validator-reg-check --chain <CHAIN> --el-url <EL_RPC_URL> --validator-info-file <VALIDATOR_INFO_JSON>`
   to check that your Shutter registrations from step 2. were successful. Note, the `--validator-info-file` is the
   `validatorInfo.json` file produced in step 2.
4. Run Erigon as usual and append the `--shutter` flag to have it run as a Shutterized Validator. This works both with
   Erigon's internal CL Caplin (on by default) and with an `--externalcl`.

## Why run it?

There are two incentives:

1. Your validator gets access to an extra source of transactions that are not available in the public devp2p based
   transaction pool. This means that your block space can be filled with additional transactions which can then lead to
   higher block rewards for you as a staker.
2. You contribute to the protection of users against malicious MEV attacks. The more Shutterized Validators exist the
   quicker the inclusion times for the encrypted transactions will be. An overview of the current inclusion times,
   shutter validator percentages, keypers available and number of shielded transactions processed can be seen
   at https://explorer.shutter.network/system-overview.
