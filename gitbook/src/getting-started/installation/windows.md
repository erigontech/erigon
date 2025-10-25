# Windows
*How to install and run Erigon 3 on Windows 10 and Windows 11*

There are 3 options for running Erigon 3 on Windows, listed from easiest to most difficult installation:

-   [Use Docker](docker.md): Run Erigon in a Docker container for isolation from the host Windows system. This avoids dependencies on Windows but requires installing Docker.
    
-   [Build executable binaries natively for Windows](windows-build-executables.md): Use the pre-built Windows executables that can be natively run on Windows without any emulation or containers required.

-   [Use Windows Subsystem for Linux (WSL)](windows-wsl.md): Install the Windows Subsystem for Linux (WSL) to create a Linux environment within Windows. Erigon can then be installed in WSL by following the Linux installation instructions. This provides compatibility with Linux builds but involves more setup overhead.