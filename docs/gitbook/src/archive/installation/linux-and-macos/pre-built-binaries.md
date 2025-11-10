---
description: Downloading and Verifying Erigon Pre-built Binaries on Linux
---

# Pre-built binaries (Linux)

## 1. Select Your Processor Architecture and Download

Go to the Erigon [releases page](https://github.com/erigontech/erigon/releases) on GitHub and select the latest stable version (e.g., <code class="expression">space.vars.version</code>) or whichever version you prefer.

<figure><img src="../../../.gitbook/assets/image (12).png" alt=""><figcaption></figcaption></figure>

Download the appropriate binary file for your processor architecture:

<table data-header-hidden><thead><tr><th width="210.88885498046875"></th><th width="185"></th><th></th></tr></thead><tbody><tr><td><strong>Processor Type</strong></td><td><strong>Binary File Type</strong></td><td><strong>Example File Name</strong></td></tr><tr><td>64-bit Intel/AMD</td><td>Debian Package (<code>.deb</code>)</td><td>erigon_<code class="expression">space.vars.version</code>_amd64.deb</td></tr><tr><td>64-bit ARM</td><td>Debian Package (<code>.deb</code>)</td><td>erigon_<code class="expression">space.vars.version</code>_arm64.deb</td></tr><tr><td>64-bit Intel/AMD</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v<code class="expression">space.vars.version</code>_linux_amd64.tar.gz</td></tr><tr><td>64-bit Intel/AMDv2</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v<code class="expression">space.vars.version</code>_amd64v2.tar.gz</td></tr><tr><td>64-bit ARM</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v<code class="expression">space.vars.version</code>_linux_arm64.tar.gz</td></tr></tbody></table>

Note that the Release Page Assets table contains also the **checksum** for each file and a checksum file.

## 2. Verifying Binary Integrity with Checksums

To verify the integrity and ensure your downloaded Erigon file hasn't been corrupted or tampered with, use the SHA256 checksums provided in the official release by following these steps:

{% stepper %}
{% step %}
#### Generate the Checksum of Your Downloaded File

Next, use the appropriate command and the name of your downloaded binary (e.g., `erigon_v3.x.x_linux_amd64.tar.gz`).

```
sha256sum <DOWNLOADED_FILE_NAME>
```

Example with a `tar.gz` file:

```bash
sha256sum erigon_v3.x.x_linux_amd64.tar.gz
```

This command will output a long string (the computed checksum) followed by the file name.
{% endstep %}

{% step %}
#### Compare the Checksums

Compare the checksum found in the previous step with those in the Assets table or the <kbd>erigon\_v3.x.x\_checksums.txt</kbd> file in the same table.

The two checksum strings must match exactly. If they do not match, the file is corrupted, and you should delete it and download it again.
{% endstep %}
{% endstepper %}

## 3. Installing the Binary Executable

After downloading and verifying the checksum, follow the instructions below based on the file type you chose.

#### **A. Using Debian Package (`.deb`)**

This method uses your distribution's package manager (like <kbd>dpkg</kbd>) to install Erigon system-wide.

1.  Navigate to the directory where you downloaded the pre-built binary, e.g. Downloads:

    ```bash
    cd ~/Downloads
    ```
2.  Install the package:

    ```bash
    sudo dpkg -i erigon_3.x.x_amd64.deb
    ```

    (Replace the filename with your downloaded version)

#### **B. Using Compressed Archive (`.tar.gz`)**

This method gives you a standalone executable that can be run from any directory.

1.  Extract the archive:

    ```bash
    tar -xzf erigon_v3.x.x_linux_amd64.tar.gz
    ```

    (Replace the filename with your downloaded version)
2.  Move the resulting `erigon` executable to a directory included in your system's <kbd>$PATH</kbd>(e.g., <kbd>$/usr/local/bin</kbd>) to run it from anywhere:

    ```bash
    sudo mv erigon /usr/local/bin/
    ```

## 4. Running Erigon

After installation, you can run Erigon from your terminal:

```bash
erigon [options]
```

See Basic Usage for more info.

{% content-ref url="../../../get-started/fundamentals/basic-usage.md" %}
[basic-usage.md](../../../get-started/fundamentals/basic-usage.md)
{% endcontent-ref %}
