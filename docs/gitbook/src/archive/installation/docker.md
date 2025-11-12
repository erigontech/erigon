---
description: How to run a Erigon node with Docker
---

# Docker

Docker is like a portable container for software. It packages Erigon and everything it needs to run, so you don't have to install complicated dependencies on your computer.

The Docker image works on **Intel/AMD computers** (linux/amd64) and **Apple Silicon Macs** (linux/arm64).

### General Info

* The Docker images feature several binaries, including: `erigon`, `downloader`, `evm`, `caplin`, `capcli`, `integration`, `rpcdaemon`, `sentry`, and `txpool`.
* The multi-platform Docker image is available for `linux/amd64/v2` and `linux/arm64` platforms and is now based on Debian Bookworm. There's no need to pull a different image for another supported platform.
* All build flags are now passed to the release workflow, allowing users to view previously missed build information in the released binaries and Docker images. This change is also expected to result in better build optimization.
* Docker images now contain the label `org.opencontainers.image.revision`, which refers to the commit ID from the Erigon project used to build the artifacts.
* With recent updates, all build configurations are now included in the release process. This provides users with more comprehensive build information for both binaries and Docker images, along with enhanced build optimizations.
* Images are stored at [https://hub.docker.com/r/erigontech/erigon](https://hub.docker.com/r/erigontech/erigon).

## Prerequisites

Install [Docker Engine](https://docs.docker.com/engine/install) if you run Linux or [Docker Desktop](https://docs.docker.com/desktop/) if you run macOS/Windows.

### Download and start Erigon in Docker

Here are the steps to download and start Erigon in Docker.

{% stepper %}
{% step %}
#### Check which version you want to download

Check in the GitHub [Release Notes](https://github.com/erigontech/erigon/releases) page which version you want to download (normally latest is the best choice).
{% endstep %}

{% step %}
#### Download Erigon container

Download the chosen version replacing `<version_tag>` with the actual version:

```sh
docker pull erigontech/erigon:<version_tag>
```

For example:

{% include "../../.gitbook/includes/docker-pull-erigontech-erig....md" %}
{% endstep %}

{% step %}
#### Start the Erigon container

Start the Erigon container in your terminal:

{% code overflow="wrap" %}
```sh
docker run -it erigontech/erigon:<version_tag> <flags>
```
{% endcode %}

For example:

{% code overflow="wrap" %}
```sh
docker run -v /erigon-data:/erigon-data -it erigontech/erigon:v3.2.2 --chain=hoodi --prune.mode=minimal --datadir /erigon-data
```
{% endcode %}

* `-v` connects a folder on your computer to the container
* `-it` lets you see what's happening and interact with Erigon
* `--chain=hoodi` specifies which [network](../../get-started/fundamentals/supported-networks.md) to sync
* `--prune.mode=minimal` tells Erigon to use minimal [Sync Mode](../../get-started/fundamentals/sync-modes.md)
* `--datadir` tells Erigon where to store data inside the container
{% endstep %}
{% endstepper %}

Additional flags can be added to configure Erigon with several options.

{% content-ref url="../../get-started/fundamentals/configuring-erigon.md" %}
[configuring-erigon.md](../../get-started/fundamentals/configuring-erigon.md)
{% endcontent-ref %}

{% include "../../.gitbook/includes/press-ctrl+c-in-the-termina....md" %}

### Optional: Setup dedicated user <a href="#optional-setup-dedicated-user" id="optional-setup-dedicated-user"></a>

#### Understanding File Permissions

When Erigon runs inside a Docker container and creates files (like its data directory), those files need to be accessible to your local user account on your host machine.

The potential issue is that Docker often creates these files with a default User ID (UID) of `1000`. If this doesn't match your host machine's UID, you may run into permission issues when trying to access, modify, or delete the data directory from your host machine.

#### The Solution: Using Your Host UID

To prevent these problems, you can run the Docker container using your local operating system's User ID (UID).

Running the container with your host machine's UID ensures that any files created or modified by Erigon inside the container will be owned by that specific user ID on the host operating system. This synchronization of permissions makes managing the data directory much easier.

If you are encountering permission issues, you can find your user ID using this command:

```bash
id -u
```

#### Example Run

To use a specific UID, like `1205`, and mount a host data directory (`/erigon-data`) into the container, use the `--user` flag:

```sh
docker run \
--user 1205 \
-v /erigon-data:/container-erigon-data \
-it erigontech/erigon:<version_tag> \
--chain=hoodi \
--prune.mode=minimal \
--datadir /container-erigon-data
```

In this example, the Erigon process inside the container will run as user `1205` and the contents of the host directory `/erigon-data` will be written and owned by user `1205` on your host OS.

### Environment Variables <a href="#environment-variables" id="environment-variables"></a>

There is a `.env.example` file in the root of the repo.

Copy

```
* DOCKER_UID - The UID of the docker user

* DOCKER_GID - The GID of the docker user

* XDG_DATA_HOME - The data directory which will be mounted to the docker containers
```

If not specified, the UID/GID will use the current user.

A good choice for `XDG_DATA_HOME` is to use the `~erigon/.ethereum` directory created by helper targets `make user_linux` or `make user_macos`.

#### Check: Permissions <a href="#check-permissions" id="check-permissions"></a>

In all cases, `XDG_DATA_HOME` (specified or default) must be writeable by the user UID/GID in docker, which will be determined by the `DOCKER_UID` and `DOCKER_GID` at build time.

If a build or service startup is failing due to permissions, check that all the directories, UID, and GID controlled by these environment variables are correct.

#### Run <a href="#run" id="run"></a>

Next command starts: `erigon` on port `30303`, `rpcdaemon` on port `8545`, `prometheus` on port `9090`, and `grafana` on port `3000`:

```bash
#
# Will mount ~/.local/share/erigon to /home/erigon/.local/share/erigon inside container
#
make docker-compose
#
# or
#
# if you want to use a custom data directory
# or, if you want to use different uid/gid for a dedicated user
#
# To solve this, pass in the uid/gid parameters into the container.
#
# DOCKER_UID: the user id
# DOCKER_GID: the group id
# XDG_DATA_HOME: the data directory (default: ~/.local/share)
#
# Note: /preferred/data/folder must be read/writeable on host OS by user with UID/GID given
#       if you followed above instructions
#
# Note: uid/gid syntax below will automatically use uid/gid of running user so this syntax
#       is intended to be run via the dedicated user setup earlier
#
DOCKER_UID=$(id -u) DOCKER_GID=$(id -g) XDG_DATA_HOME=/preferred/data/folder DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
#
# if you want to run the docker, but you are not logged in as the $ERIGON_USER
# then you'll need to adjust the syntax above to grab the correct uid/gid
#
# To run the command via another user, use
#
ERIGON_USER=erigon
sudo -u ${ERIGON_USER} DOCKER_UID=$(id -u ${ERIGON_USER}) DOCKER_GID=$(id -g ${ERIGON_USER}) XDG_DATA_HOME=~${ERIGON_USER}/.ethereum DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
```

`makefile` creates the initial directories for `erigon`, `prometheus` and `grafana`. The PID namespace is shared between erigon and rpcdaemon which is required to open Erigon's DB from another process (RPCDaemon local-mode). See: [https://github.com/ledgerwatch/erigon/pull/2392/files](https://github.com/ledgerwatch/erigon/pull/2392/files)

If your docker installation requires the docker daemon to run as root (which is by default), you will need to prefix the command above with `sudo`. However, it is sometimes recommended running docker (and therefore its containers) as a non-root user for security reasons. For more information about how to do this, refer to this [article](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).
