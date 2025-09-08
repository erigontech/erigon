# Upgrading from a previous version

Updating to the latest version of Erigon gives you access to the latest features and ensures optimal performance and stability.

## General Recommendations Before Upgrade

- **Read Release Notes**: Carefully review the [Release Notes](https://github.com/erigontech/erigon/releases) for breaking changes and new features relevant to your setup.
- **Terminate your Erigon**: End your current Erigon session by pressing `CTRL+C`.
- **Backup**: Always back up your `datadir` before performing major upgrades.  

## Managing your Data

Erigon 3.1 introduces a new snapshot format while continuing to support the old one. This means that new releases are fully compatible with your existing data. However, users who want the latest data files and data-specific fixes can perform an **optional** manual data upgrade:

1. Backup your datadir.
2. [Upgrade your Erigon installation](#upgrading-your-erigon-installation) whether from a binary, compiled source code, or Docker.
3. To initiate the data upgrade, use the following command: `./build/bin/erigon snapshots reset --datadir /your/datadir`.
4. Run Erigon, it will reuse existing data and sync only newer snapshots.

### Snapshots Upgrade Options

* `erigon update-to-new-ver-format --datadir /your/datadir`: this option updates snapshots to be compatible with latest version, but you will not get the full benefits of the new snapshots.
* `erigon snapshots reset --datadir /your/datadir`: this command removes all old snapshots that have had performance improvements.

Choose `upgrade` for a quicker process, or `reset` for maximum performance. If you choose `reset`, you'll need to wait for the new snapshots to download once Erigon starts.

> For more information see also [Why Upgrading Erigon Doesn't Always Fix Your Data](#why-upgrading-erigon-doesnt-always-fix-your-data).

### Snapshots Downgrade Options

If upgrading snapshots(`3.0`to `3.1`) now happens automatically, you should follow these instructions for downgrading:

> **WARNING**: This algorithm will remove incompatible `3.1` snapshot files because they are not backward-compatible.

1. Make sure that you're running erigon on 3.1.x version, use `erigon --version`.
2. Run `erigon --datadir ../your/datadir reset-to-old-ver-format` to reset your snapshots to old format.
3. `git checkout v3.0.x` to checkout to preferred `3.0` version. For example now latest: `git checkout v3.0.15`
4. Run your old version of Erigon.


## Upgrading your Erigon Installation

Follow the below instructions depending on your installation method:

- [Pre-built binaries](#pre-built-binaries)
- [Docker](#docker)
- [Compiled source code](#compiled-from-source)

### Pre-built Binaries

Download the latest binary file from <https://github.com/erigontech/erigon/releases>, do the [checksum](../installation/prebuilt.md#checksums) and reinstall it, no other operation needed.

### Docker

If you're using Docker to run Erigon, the process to upgrade to a newer version of the software is straightforward and revolves around pulling the latest Docker image and then running it. Here's how you can upgrade Erigon using Docker:

* **Pull the Latest Docker Image**: First, find out the tag of the new release from the [Erigon Docker Hub](https://hub.docker.com/r/erigontech/erigon). Once you know the tag, pull the new image:

    ```bash
    docker pull erigontech/erigon:<new_version_tag>
    ```

    Replace <new_version_tag> with the actual version tag you wish to use. For example:

    ```bash
    docker pull erigontech/erigon:v3.1.0-rc1
    ```


* **List Your Docker Images**: Check your downloaded images to confirm the new image is there and get the new image ID:

    ```bash
    docker images
    ```


* **Stop the Running Erigon Container**: If you have a currently running Erigon container, you'll need to stop it before you can start the new version. First, find the container ID by listing the running containers:

    ```bash
    docker ps
    ```

    Then stop the container using:

    ```bash
    docker stop <container_id>
    ```

    Replace `<container_id>` with the actual ID of the container running Erigon.

* **Remove the Old Container**: (Optional) If you want to clean up, you can remove the old container after stopping it:

    ```bash
    docker rm <container_id>
    ```

* **Run the New Image**: Now you can start a new container with the new Erigon version using the new image ID:

    ```bash
    docker run -it <new_image_id> [options]
    ```

* **Verify Operation**: Ensure that Erigon starts correctly and connects to the desired network, verifying the logs for any initial errors.

By following these steps, you'll keep your Docker setup clean and up-to-date with the latest Erigon version without needing to manually clean up or reconfigure your environment.

### Compiled from source

To upgrade Erigon to a newer version when you've originally installed it via Git and manual compilation, you should follow these steps without needing to delete the entire folder:

* **Navigate to your Erigon directory**:

    For example:

    ```bash
    cd erigon
    ```

* **Fetch the latest changes from the repository**: You need to make sure your local repository is up-to-date with the main GitHub repository. Run:

    ```bash
    git fetch --tags
    ```

* **Check out** the [latest version](https://github.com/erigontech/erigon/releases) and switch to it using:


    ```bash
    git checkout <new_version_tag>
    ```

    Replace `<new_version_tag>` with the version tag of the new release, for example:

    ```bash
    git checkout v3.1.0
    ```

* **Rebuild Erigon**: Since the codebase has changed, you need to compile the new version. Run:

    ```bash
    make erigon
    ```
   
This process updates your installation to the latest version you specify, while maintaining your existing data. You're essentially just replacing the executable with a newer version.

## Why Upgrading Erigon Doesn't Always Fix Your Data

Upgrading Erigon involves a key distinction between its core software and its data files, which are managed separately. This approach is rooted in practicality and user control.

The Erigon **software** is the application that processes and interacts with blockchain data. However, the majority of the data itself, including the state of the network, exists in **data files** that you download and store locally.

If a bug is discovered, it is often in the data itself rather than a flaw in the Erigon code. In such a case, simply updating the Erigon binary won't resolve the issue because the faulty data remains on your disk. This is because Erigon upgrades do not normally alter the data files. You must reset and re-download the data snapshots to get the corrected files.

This separation prevents unnecessary and time-consuming processes. The Erigon team cannot regenerate months of data for every minor bug fix, and users shouldn't have to re-download terabytes of information with every new weekly software release.

This design also provides flexibility. A user might need a specific data fix but prefer to remain on an older software version due to a known bug or regression in the latest release. Similarly, a user might be satisfied with their current data set and only need to update the Erigon binary.

While this dual-versioning system may seem complex, it is a deliberate design choice that optimizes for both efficiency and user autonomy.

