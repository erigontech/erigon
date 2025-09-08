# Install Erigon with Pre-built Binaries

You can download and install the latest version of Erigon (e.g., v3.1.0) directly from our GitHub releases page.

## 1. Select Your Platform and Download

Go to the [Erigon releases page](https://github.com/erigontech/erigon/releases) and select the latest version (or whichever version you prefer).

<img src="/images/releases.png" alt="Erigon Releases" style="display: block; margin: 0 auto;">

Then, download the appropriate binary for your system:

- For **Linux**:
  - `erigon_3.x.x_amd64.deb` for 64-bit Intel/AMD processors
  - `erigon_3.x.x_arm64.deb` for 64-bit ARM processors
  - `erigon_v3.x.x_linux_amd64.tar.gz` for 64-bit Intel/AMD processors
  - `erigon_v3.x.x_linux_arm64.tar.gz` for 64-bit ARM processors
- For **MacOS**:
    - `erigon_3.x.x_darwin_amd64.tar.gz` for 64-bit Intel/AMD processors
    - `erigon_3.x.x_darwin_arm64.tar.gz` for 64-bit ARM processors

### Checksums

To verify the integrity of the downloaded file, you can use the checksums provided in the `erigon_v3.x.x_checksums.txt` file. This file contains SHA256 checksums for all Erigon binaries.

```bash
BASE="https://github.com/erigontech/erigon/releases/download/v3.x.x"
wget $BASE/erigon_v3.x.x_checksums.txt
# then one of:
wget $BASE/erigon_3.x.x_amd64.deb
# or
wget $BASE/erigon_3.x.x_arm64.deb
# or
wget $BASE/erigon_v3.x.x_linux_amd64.tar.gz
# or
wget $BASE/erigon_v3.x.x_linux_arm64.tar.gz
```