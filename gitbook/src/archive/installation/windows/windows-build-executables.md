---
description: Build Executable Binaries Natively for Windows
---

# Native compilation

## Software Prerequisites

You must install the following software and set the below environment variables before compiling Erigon.

### Chocolatey

Install _Chocolatey package manager_ by following these [instructions](https://docs.chocolatey.org/en-us/choco/setup).

Once your Windows machine has the above installed, open the **Command Prompt** by typing "**cmd**" in the search bar and check that you have correctly installed Chocolatey:

```bash
choco -v
```

<figure><img src="../../../.gitbook/assets/image (2).png" alt=""><figcaption></figcaption></figure>

### cmake, make, mingw

Now you need to install the following components: `cmake`, `make`, `mingw` by:

```bash
choco install cmake make mingw
```

{% hint style="warning" %}
**Important note about Anti-Virus:**

During the compiler detection phase of **MinGW**, some temporary executable files are generated to test the compiler capabilities. It's been reported that some anti-virus programs detect these files as possibly infected with the `Win64/Kryptic.CIS` Trojan horse (or a variant of it). Although these are false positives, we have no control over the 100+ vendors of security products for Windows and their respective detection algorithms and we understand that this may make your experience with Windows builds uncomfortable. To work around this, you can either set exclusions for your antivirus software specifically for the`build\bin\mdbx\CMakeFiles` subfolder of the cloned repo, or you can run Erigon using the other two options below.
{% endhint %}

### Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

### **Set the System Environment Variable**

Make sure that the Windows System Path variable is set correctly. Use the search bar on your computer to search for “**Edit the system environment variable**”.

<figure><img src="../../../.gitbook/assets/image (3).png" alt=""><figcaption></figcaption></figure>

Click the “**Environment Variables...**” button.

<figure><img src="../../../.gitbook/assets/image (4).png" alt=""><figcaption></figcaption></figure>

Look down at the "**System variables**" box and double click on "**Path**" to add a new path.

<figure><img src="../../../.gitbook/assets/image (5).png" alt=""><figcaption></figcaption></figure>

Then click on the "**New**" button and paste the following path:

```bash
 C:\ProgramData\chocolatey\lib\mingw\tools\install\mingw64\bin
```

<figure><img src="../../../.gitbook/assets/image (6).png" alt=""><figcaption></figcaption></figure>

## 2. Clone the Erigon repository

Open the Command Prompt and type the following:

{% include "../../../.gitbook/includes/git-clone-https-github.co....md" %}

Next, fetch all available release tags:

{% include "../../../.gitbook/includes/git-fetch-tags.md" %}

Check out the desired version tag by replacing `<tag_name>` with the version you want. Normally latest stable version is the best, check the official [Release Notes](https://github.com/erigontech/erigon/releases). For example:

{% include "../../../.gitbook/includes/git-checkout-version.md" %}

You might need to change the `ExecutionPolicy` to allow scripts created locally or signed by a trusted publisher to run. Open a Powershell session as Administrator and type:

```powershell
Set-ExecutionPolicy RemoteSigned
```

## 3. Compiling Erigon

This section outlines the two primary methods for compiling the Erigon client and its associated modules directly from the source code on a Windows environment. Compiling from source ensures you are running the latest version and gives you control over the final binaries.

You have two alternative options for compilation, both utilizing PowerShell: a quick, graphical method via File Explorer, and a more controlled, command-line method. All successfully compiled binaries will be placed in the `.\build\bin\` subfolder of your Erigon directory.

| **Method**                  | **Pro**                                                                                                           | **Con**                                                                                                |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| File Explorer (`wmake.ps1`) | Fastest and simplest; requires minimal command-line interaction.                                                  | Less control over which specific component is built (builds all modules by default).                   |
|  PowerShell CLI             | Provides granular control, allowing you to compile only specific components (e.g., just `erigon` or `rpcdaemon`). | Requires CLI familiarity and an additional step to modify the `ExecutionPolicy` for script permission. |

{% tabs %}
{% tab title="File Explorer " %}
This is the fastest way which normally works for everyone. Open the File Explorer and go to the Erigon folder, then right click the `wmake` file and choose "**Run with PowerShell**".

<figure><img src="../../../.gitbook/assets/image (7).png" alt=""><figcaption></figcaption></figure>

PowerShell will compile Erigon and all of its modules. All binaries will be placed in the `.\build\bin\` subfolder.

<figure><img src="../../../.gitbook/assets/image (8).png" alt=""><figcaption></figcaption></figure>
{% endtab %}

{% tab title=" PowerShell CLI" %}
In the search bar on your computer, search for “**Windows PowerShell**” and open it.

<figure><img src="../../../.gitbook/assets/image (9).png" alt=""><figcaption></figcaption></figure>

Change the working directory to "**erigon**"

```bash
cd erigon
```

<figure><img src="../../../.gitbook/assets/image (10).png" alt=""><figcaption></figcaption></figure>

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

<figure><img src="../../../.gitbook/assets/image (11).png" alt=""><figcaption></figcaption></figure>

The executable binary `erigon.exe` should have been created in the `.\build\bin\` subfolder.

You can use the same command to build other binaries such as `RPCDaemon`, `TxPool`, `Sentry` and `Downloader`.
{% endtab %}
{% endtabs %}

## 4. Running Erigon

To start Erigon place your command prompt in the `.\build\bin\` subfolder and use:

```powershell
start erigon.exe
```

or from any place use the full address of the executable:

```powershell
start C:\Users\username\AppData\Local\erigon.exe
```

See [basic usage](../../../get-started/fundamentals/basic-usage.md) documentation on available options to [customize](../../../get-started/fundamentals/configuring-erigon.md) your Erigon experience.

{% content-ref url="../../../get-started/fundamentals/basic-usage.md" %}
[basic-usage.md](../../../get-started/fundamentals/basic-usage.md)
{% endcontent-ref %}
