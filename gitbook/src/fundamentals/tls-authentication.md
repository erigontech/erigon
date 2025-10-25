---
description: >-
  Securing Erigon Communication with TLS Authentication: A Step-by-Step OpenSSL
  Guide
---

# TLS Authentication

## Introduction

TLS authentication can be enabled to ensure communication integrity and access control to the Erigon node.

At a high level, the process consists of:

1. [Generate the Certificate Authority (CA) key pair](tls-authentication.md#1-generating-the-key-pair-for-the-certificate-authority-ca)
2. [Create the Certificate Authority certificate file](tls-authentication.md#2-creating-the-ca-certificate-file)
3. [Generate a key pair](tls-authentication.md#3-generating-a-key-pair)
4. [Create the certificate file for each public key](tls-authentication.md#4-creating-the-certificate-file-for-each-public-key)
5. [Deploy the files to each instance](tls-authentication.md#5-deploy-the-files-on-each-instance)
6. [Run Erigon and RPCdaemon with the correct tags](tls-authentication.md#6-run-erigon-and-rpcdaemon-with-the-correct-tags)

The following is a detailed description of how to use the **OpenSSL** suite of tools to secure the connection between a remote Erigon node and a remote or local RPCdaemon. The same procedure applies to any Erigon component you wish to run separately; it is recommended to name the files accordingly.

{% hint style="warning" %}
**Warning**: To maintain a high level of security, it is recommended to create all the keys locally and then copy the 3 required files remotely to the remote node.
{% endhint %}

### Prerequisites

Make sure you have [openssl](https://openssl-library.org/source/) installed.

### Notes

Normally, the "client side" (in our case, the RPCdaemon) will check that the server's host name matches the "Common Name" attribute of the "server" certificate. At this time, this check is disabled and will be re-enabled when the instructions above on how to correctly generate Common Name certificates are updated. For example, if you are running the Erigon instance in the Google Cloud, you will need to specify the internal IP in the `-private.api.addr` option. You will also need to open the firewall on the port you use to connect to the Erigon instances.

## 1. Generating the key pair for the Certificate Authority (CA)

Generate the CA key pair using Elliptic Curve (as opposed to RSA). The generated CA key will be in the `CA-key.pem` file.

{% hint style="warning" %}
**Warning**: Access to this file will allow anyone to later add any new instance key pair to the “cluster of trust”, so keep this file safe.
{% endhint %}

```bash
openssl ecparam -name prime256v1 -genkey -noout -out CA-key.pem
```

## 2. Creating the CA certificate file

Create CA self-signed certificate (this command will ask questions, the answers aren’t important for now, but at least the first one needs to be filled in with some data). The file created by this command will be called `CA-cert.pem`:

```bash
openssl req -x509 -new -nodes -key CA-key.pem -sha256 -days 3650 -out CA-cert.pem
```

## 3. Generating a key pair

Generate a key pair for the Erigon node:

```bash
openssl ecparam -name prime256v1 -genkey -noout -out erigon-key.pem
```

Also generate a key pair for the RPC daemon:

```bash
openssl ecparam -name prime256v1 -genkey -noout -out RPC-key.pem
```

## 4. Creating the certificate file for each public key

Now create the Certificate Signing Request for the Erigon key pair, and from this request, produce the certificate (signed by the CA) that proves that this key is now part of the “cluster of trust”:

```bash
openssl x509 -req -in erigon.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out erigon.crt -days 3650 -sha256
```

Then create the certificate signing request for the RPC daemon key pair:

```bash
openssl req -new -key RPC-key.pem -out RPC.csr
```

From this request, produce the certificate (signed by CA), proving that this key is now part of the “cluster of trust”:

```bash
openssl x509 -req -in RPC.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out RPC.crt -days 3650 -sha256
```

## 5. Deploy the files on each instance

These three files must be placed in the /erigon folder on the machine running Erigon:

`CA-cert.pem`

`erigon-key.pem`

`erigon.crt`

On the RPCdaemon machine, these three files must also be placed in the /erigon folder:

`CA-cert.pem`

`RPC key.pem`

`RPC.crtv`

## 6. Run Erigon and RPCdaemon with the correct tags

Once all the files have been moved, Erigon must be run with these additional options:

```bash
--tls --tls.cacert CA-cert.pem --tls.key erigon-key.pem --tls.cert erigon.crt
```

While the RPC daemon must be started with these additional options:

```bash
--tls.key RPC-key.pem --tls.cacert CA-cert.pem --tls.cert RPC.crt
```
