---
description: How to build Erigon in Linux and MacOS from source
---

# Build Erigon from source

The basic Erigon configuration is suitable for most users who simply want to run a node. To ensure you are building a specific, stable release, use Git tags.

####

{% stepper %}
{% step %}
### Clone the Erigon repository

First, clone the Erigon repository (you do not need to specify a branch):

{% include "../../.gitbook/includes/git-clone-https-github.co....md" %}
{% endstep %}

{% step %}
### Check Out the Desired Stable Version (Tag)

Next, fetch all available release tags:

{% include "../../.gitbook/includes/git-fetch-tags.md" %}

Check out the desired version tag by replacing `<tag_name>` with the version you want. Normally latest stable version is the best, check the official [Release Notes](https://github.com/erigontech/erigon/releases). For example:

{% include "../../.gitbook/includes/git-checkout-version.md" %}
{% endstep %}

{% step %}
### Compile the Software

Compile the Erigon binary using the `make` command.

Standard Compilation:

```bash
make erigon
```

Fast Compilation (Recommended): to significantly speed up the process, specify the number of processors you want to use with the `-j<n>` option, where `<n>` is the number of processor you want to use (we recommend using a number slightly less than your total core count):

```bash
make -j<n> erigon
```
{% endstep %}
{% endstepper %}

The resulting executable binary will be created in the `./build/bin/erigon` path.
