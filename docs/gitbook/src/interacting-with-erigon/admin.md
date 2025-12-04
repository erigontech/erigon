---
description: Security-Sensitive Methods for Node Operators
---

# admin

The `admin` namespace provides administrative methods for managing the Erigon node, including peer management and node information retrieval. These methods are designed for node operators and developers who need to monitor and control various aspects of the Erigon client's operation.

The admin namespace must be explicitly enabled using the `--http.api` flag when starting the RPC daemon. For security reasons, it's recommended not to include `admin` in the API list for public RPC endpoints.

### Security Considerations

* The admin namespace provides powerful administrative functions that should not be exposed on public RPC endpoints
* When configuring public RPC access, explicitly exclude `admin` from the `--http.api` flag to prevent unauthorized access
* These methods can affect node connectivity and should only be used by trusted operators

### Usage in Testing and Development

* Admin methods are commonly used in automated testing environments for peer management
* The docker-compose configuration for automated testing includes the admin namespace in the API list for testing purposes
* These methods are essential for setting up test networks and managing peer connections programmatically

### Availability

* Admin methods are available when the `admin` namespace is included in the `--http.api` flag
* All admin methods are available on both HTTP and WebSocket connections
* Some admin methods may require the node to be running with specific network configurations

### Integration with P2P Network

* Admin methods interact directly with Erigon's P2P networking layer
* Peer management operations may take time to complete as they involve network operations
* The effectiveness of `admin_addPeer` depends on network connectivity and peer availability

***

## **admin\_nodeInfo**

Returns information about the running node, including network details, protocols, and node identification.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                                     |
| ------ | --------------------------------------------------------------- |
| Object | Node information object containing network and protocol details |

***

## **admin\_peers**

Returns information about connected peers, including their network addresses, protocols, and connection status.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"admin_peers","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                                          |
| ----- | -------------------------------------------------------------------- |
| Array | Array of peer objects containing connection and protocol information |

***

## **admin\_addPeer**

Attempts to add a new peer to the node's peer list by connecting to the specified enode URL.

**Parameters**

| Parameter | Type   | Description                                                       |
| --------- | ------ | ----------------------------------------------------------------- |
| enode     | STRING | The enode URL of the peer to add (format: enode://pubkey@ip:port) |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"admin_addPeer","params":["enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type    | Description                                              |
| ------- | -------------------------------------------------------- |
| Boolean | True if the peer was successfully added, false otherwise |

***

## **admin\_removePeer**

Removes a peer from the node's peer list by disconnecting from the specified enode URL.

**Parameters**

| Parameter | Type   | Description                                                          |
| --------- | ------ | -------------------------------------------------------------------- |
| enode     | STRING | The enode URL of the peer to remove (format: enode://pubkey@ip:port) |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"admin_removePeer","params":["enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type    | Description                                                |
| ------- | ---------------------------------------------------------- |
| Boolean | True if the peer was successfully removed, false otherwise |

***
