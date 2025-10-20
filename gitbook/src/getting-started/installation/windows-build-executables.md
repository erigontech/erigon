# Build executable binaries natively for Windows

Before proceeding, ensure that the [hardware](../hardware-requirements.md) and [software](../software-requirements.md) requirements are met.

## Installing Chocolatey

Install _Chocolatey package manager_ by following these [instructions](https://docs.chocolatey.org/en-us/choco/setup).

Once your Windows machine has the above installed, open the **Command Prompt** by typing "**cmd**" in the search bar and check that you have correctly installed Chocolatey:

```bash
choco -v
```

Now you need to install the following components: `cmake`, `make`, `mingw` by:

```bash
choco install cmake make mingw
```

**Important note about Anti-Virus:**

During the compiler detection phase of **MinGW**, some temporary executable files are generated to test the compiler capabilities. It's been reported that some anti-virus programs detect these files as possibly infected with the `Win64/Kryptic.CIS` Trojan horse (or a variant of it). Although these are false positives, we have no control over the 100+ vendors of security products for Windows and their respective detection algorithms and we understand that this may make your experience with Windows builds uncomfortable. To work around this, you can either set exclusions for your antivirus software specifically for the`build\bin\mdbx\CMakeFiles` subfolder of the cloned repo, or you can run Erigon using the other two options below.

Make sure that the Windows System Path variable is set correctly. Use the search bar on your computer to search for “**Edit the system environment variable**”.

![](../../../../images/Edit_sys_env.png)

Click the “**Environment Variables...**” button.

![](../../../../images/Edit_sys_env2.png)

Look down at the "**System variables**" box and double click on "**Path**" to add a new path.

![](../../../../images/System_var.png)

Then click on the "**New**" button and paste the following path:

```bash
 C:\ProgramData\chocolatey\lib\mingw\tools\install\mingw64\bin
```

![](../../../../images/new_sys_var.png)

## Clone the Erigon repository

Open the Command Prompt and type the following:

```bash
git clone --branch release/3.1 --single-branch https://github.com/erigontech/erigon.git
```

You might need to change the `ExecutionPolicy` to allow scripts created locally or signed by a trusted publisher to run. Open a Powershell session as Administrator and type:

```powershell
Set-ExecutionPolicy RemoteSigned
```

## Compiling Erigon

To compile Erigon there are two alternative methods:

1. [Compiling from the wmake.ps1 file in the File Explorer](windows-build-executables.md#1-compiling-from-the-wmakeps1-file-in-the-file-explorer)
2. [Using the PowerShell CLI](windows-build-executables.md#2-using-the-powershell-cli)

### 1. Compiling from the wmake.ps1 file in the File Explorer

This is the fastest way which normally works for everyone. Open the File Explorer and go to the Erigon folder, then right click the `wmake` file and choose "**Run with PowerShell**".

![](../../../../images/powershell.png)

PowerShell will compile Erigon and all of its modules. All binaries will be placed in the `.\build\bin\` subfolder.

![](../../../../images/powershell2.png)

### 2. Using the PowerShell CLI

In the search bar on your computer, search for “**Windows PowerShell**” and open it.

![](../../../../images/powershell3.png)

Change the working directory to "**erigon**"

```bash
cd erigon
```

![](../../../../images/powershell4.png)

Before modifying security settings, ensure PowerShell script execution is allowed in your Windows account settings using the following command:

```powershell
Set-ExecutionPolicy Bypass -Scope CurrentUser -Force
```

This change allows script execution, but use caution to avoid security risks. Remember to only make these adjustments if you trust the scripts you intend to run. Unauthorized changes can impact system security. For more info read [Set-Execution Policy](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.security/set-executionpolicy?view=powershell-7.3) documentation.

Now you can compile Erigon and/or any of its component:

```powershell
.\wmake.ps1 [-target] <targetname>
```

For example, to build the Erigon executable write:

```powershell
.\wmake.ps1 erigon
```

![](../../../../images/powershell5.png)

The executable binary `erigon.exe` should have been created in the `.\build\bin\` subfolder.

You can use the same command to build other binaries such as `RPCDaemon`, `TxPool`, `Sentry` and `Downloader`.

## Running Erigon

To start Erigon place your command prompt in the `.\build\bin\` subfolder and use:

```powershell
start erigon.exe.
```

or from any place use the full address of the executable:

```powershell
start C:\Users\username\AppData\Local\erigon.exe
```

See [basic usage](../../fundamentals/basic-usage.md) documentation on available options and flags to customize your Erigon experience.
