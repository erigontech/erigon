==================
Installation Guide
==================

Prerequisite
============

To follow up this guide you need to have Golang installed, since we are going to use it to compile the code itself. you can find a tutorial on how to install Golang here: https://golang.org/doc/install.

Additionally, you may want to have git installed (but you can also download directly from Github): https://git-scm.com/book/en/v2/Getting-Started-Installing-Git.

Compiling
=========

In order to download the code and compile you need the following commands:

.. code-block:: sh

    $ git clone https://github.com/erigontech/erigon

    $ cd erigon

    $ make

Running
=======

After this is done, you can run Erigon by executing.

.. code-block:: sh

    $ ./build/bin/erigon

what this will do is start a sync process in mainnet.

RPC Daemon
==========

Unlike Go-Ethereum (geth), Erigon has a separate RPC service called rpcdaemon, this service is used for managing JSON RPC API. In fact, they are not present in Erigon with `--rpc` flag but we have to start a separate service for it. first of all Erigon must give access to the rpcdaemon to the database through an API. so we need to run Erigon with the flag **--private.api.addr** which by convention should be set to **localhost:9090**.

.. code-block:: sh

    $ ./build/bin/erigon --private.api.addr=localhost:9090

then to run the rpcdaemon attached to our node we just run

.. code-block:: sh

    $ ./build/bin/rpcdaemon

now we can make use of the JSON RPC API at localhost:8545. in order to change the address in which we serve the API we can use the flag **--http.api=<something>**