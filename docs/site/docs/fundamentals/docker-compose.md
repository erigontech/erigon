---
title: "Docker Compose"
description: "Running Erigon and all its modules with Docker Compose."
sidebar_position: 23
---

# Docker Compose

#### Understanding File Permissions

When Erigon runs inside a Docker container and creates files (like its data directory), those files need to be accessible to your local user account on your host machine.

The Erigon image runs as user ID (UID) `1000` and group ID (GID) `1000` by default. For a host-mounted data directory, use the UID and GID that own the directory and its files.

#### The Solution: Using Your Host UID and GID

Pass both IDs as `--user UID:GID`. Numeric IDs do not need corresponding named accounts inside the image.

If you specify only a UID that has no account inside the image, Docker uses GID `0`. Database access may work because the UID matches, but compaction can fail when it tries to preserve the database's original group.

Find your host user's UID and GID with:

```bash
id -u
id -g
```

#### Example Run

For a data directory (`/erigon-data`) owned by your current host user and primary group:

```sh
docker run \
--user "$(id -u):$(id -g)" \
-v /erigon-data:/container-erigon-data \
-it erigontech/erigon:<version_tag> \
--chain=hoodi \
--prune.mode=minimal \
--datadir /container-erigon-data
```

The container runs with your host user's UID and GID. If an existing data directory belongs to another account, use that account's UID and GID instead.

### Environment Variables

There is a `.env.example` file in the root of the repo.

Copy

```text
* DOCKER_UID - The UID of the docker user

* DOCKER_GID - The GID of the docker user

* XDG_DATA_HOME - The data directory which will be mounted to the docker containers
```

If not specified, the UID/GID will use the current user.

A good choice for `XDG_DATA_HOME` is to use the `~erigon/.ethereum` directory created by helper targets `make user_linux` or `make user_macos`.

#### Check: Permissions

In all cases, `XDG_DATA_HOME` (specified or default) must be writeable by the user UID/GID in Docker, which will be determined by the `DOCKER_UID` and `DOCKER_GID` at build time.

If a build or service startup is failing due to permissions, check that all the directories, UID, and GID controlled by these environment variables are correct.

#### Run

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

`Makefile` creates the initial directories for `erigon`, `prometheus` and `grafana`. The PID namespace is shared between Erigon and RPC Daemon which is required to open Erigon's DB from another process (RPC Daemon local-mode). See: [https://github.com/erigontech/erigon/pull/2392/files](https://github.com/erigontech/erigon/pull/2392/files)

If your Docker installation requires the Docker daemon to run as root (which is by default), you will need to prefix the command above with `sudo`. However, it is sometimes recommended running Docker (and therefore its containers) as a non-root user for security reasons. For more information about how to do this, refer to this [article](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).
