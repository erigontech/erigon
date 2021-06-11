<#
   Copyright 2021 The Erigon Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
#>

$gitErrorText = @"

 Requirement Error.
 You need to have Git installed
 Please visit https://git-scm.com/downloads and download the appropriate
 installer.

"@


$goErrorText = @"

 Requirement Error.
 You need to have Go Programming Language (aka golang) installed.
 Minimum required version is 1.16
 Please visit https://golang.org/dl/ and download the appropriate
 installer.

"@

$chocolateyErrorText = @"

 Requirement Error.
 For this script to run properly you need to install
 chocolatey [https://chocolatey.org/] with the following
 mandatory components: 

 - cmake 3.20.2
 - make 4.3
 - mingw 10.2.0

"@

$chocolateyPathErrorText = @"

 Environment PATH Error.
 Chocolatey install has been detected but the environment
 variable PATH does not include a full path to its binaries
 Please amend your setup and ensure the following
 chocolatey directory is properly inserted into your PATH
 environment variable.

"@

$privilegeErrorText = @"

 Privileges Error !
 You must run this script with Administrator privileges

"@

$Error.Clear()
$ErrorActionPreference = "SilentlyContinue"

Set-Variable -Name "MyContext" -Value ([hashtable]::Synchronized(@{})) -Scope Script
$MyContext.Name       = $MyInvocation.MyCommand.Name
$MyContext.Definition = $MyInvocation.MyCommand.Definition
$MyContext.Directory  = (Split-Path (Resolve-Path $MyInvocation.MyCommand.Definition) -Parent)
$MyContext.StartDir   = (Get-Location -PSProvider FileSystem).ProviderPath
$MyContext.WinVer     = (Get-WmiObject Win32_OperatingSystem).Version.Split(".")
$MyContext.PSVer      = [int]$PSVersionTable.PSVersion.Major

# -----------------------------------------------------------------------------
# Function 		: Test-Administrator
# -----------------------------------------------------------------------------
# Description	: Checks the script is running with Administrator privileges
# Returns       : $true / $false
# -----------------------------------------------------------------------------
function Test-Administrator {

    $myWindowsID = [System.Security.Principal.WindowsIdentity]::GetCurrent();
    $myWindowsPrincipal = New-Object System.Security.Principal.WindowsPrincipal($myWindowsID);
    $adminRole = [System.Security.Principal.WindowsBuiltInRole]::Administrator;
    Write-Output ($myWindowsPrincipal.IsInRole($adminRole))
} 

# -----------------------------------------------------------------------------
# Function 		: Test-Valid-Env
# -----------------------------------------------------------------------------
# Description	: Checks the named variable provided is present in env:
# Returns       : $true / $false
# -----------------------------------------------------------------------------
function Get-Env {
    param ([string]$varName = $(throw "A variable name must be provided"))
    $result = Get-ChildItem env: | Where-Object {$_.Name -ieq $varName}
    if (-not $result) {
        Write-Output $null
    } else {
        Write-Output $result.Value
    }
}

# -----------------------------------------------------------------------------
# Function 		: Get-Uninstall-Item
# -----------------------------------------------------------------------------
# Description	: Try get uninstall key for given item pattern
# Returns       : object
# -----------------------------------------------------------------------------
function Get-Uninstall-Item {
    param ([string]$pattern = $(throw "A search pattern must be provided"))    
    
    # Trying to get the enumerable of all installed programs using Get-ItemProperty may cause 
    # exceptions due to possible garbage values insterted into the registry by installers.
    # Specifically an invalid cast exception throws when registry keys contain invalid DWORD data. 
    # See https://github.com/PowerShell/PowerShell/issues/9552
    # Due to this all items must be parsed one by one

    $Private:regUninstallPath = "HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\"
    $Private:result = $null
    Get-ChildItem -Path $regUninstallPath | ForEach-Object {
        if(-not $result) {
            $item = Get-ItemProperty -Path $_.pspath
            if ($item.DisplayName -imatch $pattern) {
                $result = $item
                # DO NOT use break
            }
        }
    }
    Write-Output $result
}

# -----------------------------------------------------------------------------
# Function 		: Test-GO-Installed
# -----------------------------------------------------------------------------
# Description	: Checks whether or not GO language is installed
# Returns       : $true / $false
# -----------------------------------------------------------------------------
function Test-GO-Installed {

    $Private:item   = Get-Uninstall-Item "^Go\ Programming\ Language"
    $Private:result = $false

    if ($Private:item) {
        $Private:versionMajor = [int]$item.VersionMajor
        $Private:versionMinor = [int]$item.VersionMinor
        if ($Private:versionMajor -ge 1 -and $Private:versionMinor -ge 16) {
            Write-Host " Found GO version $($Private:item.DisplayVersion)"
            $Private:result = $true
        }
    }

    Write-Output $Private:result
}

# -----------------------------------------------------------------------------
# Function 		: Test-Git-Installed
# -----------------------------------------------------------------------------
# Description	: Checks whether or not Git is installed
# Returns       : $true / $false
# -----------------------------------------------------------------------------
function Test-Git-Installed {

    $Private:item   = Get-Uninstall-Item "^Git\ "
    $Private:result = $false

    if ($Private:item) {
        Write-Host " Found Git version $($Private:item.DisplayVersion)"
        $Private:result = $true
    }

    Write-Output $Private:result
}

# Test requirements

## Test Git is installed
if(!(Test-Git-Installed)) {
    Write-Host $gitErrorText
    return
}
Get-Command git.exe | Out-Null
if (!($?)) {
    Write-Host @"
    
 Error !
 Though Git installation is found I could not get
 the Git binary executable. Ensure Git installation
 directory is properly inserted into your PATH
 environment variable.

"@
    return
}

## GO language is installed
if(!(Test-GO-Installed)) {
    Write-Host $goErrorText
    return
}
Get-Command go.exe | Out-Null
if (!($?)) {
    Write-Host @"
    
 Error !
 Though GO installation is found I could not get
 the GO binary executable. Ensure GO installation
 directory is properly inserted into your PATH
 environment variable.

"@
    return
}

## Test Chocolatey Install
$chocolateyPath = Get-Env "chocolateyInstall"
if(-not $chocolateyPath) {
    Write-Host $chocolateyErrorText
    return
}

# Test Chocolatey bin directory is actually in %PATH%
$chocolateyBinPath = (Join-Path $chocolateyPath "bin")
$chocolateyBinPathInPath = $false
$pathExpanded = $env:Path.Split(";")
for($i=0; $i -lt $pathExpanded.Count; $i++){
    $pathItem = $pathExpanded[$i]
    if($pathItem -ieq $chocolateyBinPath){
        Write-Host " Found $($chocolateyBinPath) in PATH"
        $chocolateyBinPathInPath = $true
    }
}
if(!$chocolateyBinPathInPath) {
    Write-Host $chocolateyPathErrorText
    Write-Host $chocolateyBinPath
    return
}

## Test Chocolatey Components
$chocolateyHasCmake = $false
$chocolateyHasMake = $false
$chocolateyHasMingw = $false
$chocolateyComponents = @(clist -l)
for($i=0; $i -lt $chocolateyComponents.Count; $i++){
    $item = $chocolateyComponents[$i]
    if($item -imatch "^cmake\ [0-9]") {
        $chocolateyHasCmake = $true
        Write-Host " Found Chocolatey component $($item)"
    }
    if($item -imatch "^make\ [0-9]") {
        $chocolateyHasMake = $true
        Write-Host " Found Chocolatey component $($item)"
    }
    if($item -imatch "^mingw\ [0-9]") {
        $chocolateyHasMingw = $true
        Write-Host " Found Chocolatey component $($item)"
    }
}
If(!$chocolateyHasCmake -or !$chocolateyHasMake -or !$chocolateyHasMingw) {
    Write-Host $chocolateyErrorText
    return
}

Get-Command cmake.exe | Out-Null
if (!($?)) {
    Write-Host @"

 Error !
 Though chocolatey cmake installation is found I could not get
 the cmake binary executable. Ensure cmake installation
 directory is properly inserted into your PATH
 environment variable.
 (Usually $(Join-Path $env:ProgramFiles "Cmake\bin"))

"@
    return
}

## Administrator Privileges
if (!(Test-Administrator)) {
   Write-Host $privilegeErrorText
   return
}

# Enter MDBX directory and build libmdbx.dll
Set-Location (Join-Path $MyContext.Directory "ethdb\mdbx\dist")

# Delete CMakeCache.txt and CMakeFiles directory if they exist
if (Test-Path "CMakeCache.txt" -PathType Leaf) {
    Write-Host " Removing MDBX CMakeCache.txt ..."
    Remove-Item -Path "CMakeCache.txt"
}
if (Test-Path "CMakeFiles") {
    Write-Host " Removing MDBX CMakeFiles ..."
    Remove-Item -Path "CMakeFiles" -Recurse -Force
}

Write-Host " Building libmdbx.dll ..."
cmake -G "MinGW Makefiles" . `
-D CMAKE_MAKE_PROGRAM:PATH=""$(Join-Path $chocolateyBinPath "make.exe")"" `
-D CMAKE_C_COMPILER:PATH=""$(Join-Path $chocolateyBinPath "gcc.exe")"" `
-D CMAKE_CXX_COMPILER:PATH=""$(Join-Path $chocolateyBinPath "g++.exe")"" `
-D CMAKE_BUILD_TYPE:STRING="Release" `
-D MDBX_BUILD_SHARED_LIBRARY:BOOL=ON `
-D MDBX_WITHOUT_MSVC_CRT:BOOOL=OFF `
-D MDBX_FORCE_ASSERTIONS:INT=0

if($LASTEXITCODE) {
    Write-Host "An error has occurred while configuring MDBX dll"
    return
}
cmake --build .
if($LASTEXITCODE -or !(Test-Path "libmdbx.dll" -PathType leaf)) {
    Write-Host "An error has occurred while building MDBX dll or libmdbx.dll cannot be found"
    return
}

# Copy libmdbx.dll into %windir%\System32 directory
# Note! default behavior is to overwrite
Copy-Item libmdbx.dll (Join-Path $env:SystemRoot system32)
if(!$?) {
   Write-Host @" 

  Error ! Could not copy libmdbx.dll to $(Join-Path $env:SystemRoot system32)
  What you can try : 
  - Check your permissions to directory
  - Check there's an already existing libmdbx.dll file
  - Check no instance of Erigon with mdbx is currently running

"@
   return
}

# Return to source folder
Set-Location $MyContext.Directory

# Build erigon binaries
Set-Variable -Name "Erigon" -Value ([hashtable]::Synchronized(@{})) -Scope Script
$Erigon.Commit  = [string]@(git.exe rev-list -1 HEAD)
$Erigon.Branch  = [string]@(git.exe rev-parse --abbrev-ref HEAD)
$Erigon.Tag     = [string]@(git.exe describe --tags)
$Erigon.Build   = "go build -v -trimpath -ldflags ""-X github.com/ledgerwatch/erigon/params.GitCommit=$($Erigon.Commit) -X github.com/ledgerwatch/erigon/params.GitBranch=$($Erigon.Branch) -X github.com/ledgerwatch/erigon/params.GitTag=$($Erigon.Tag)"""
$Erigon.BinPath = [string](Join-Path $MyContext.StartDir "\build\bin")
$env:GO111MODULE = "on"

# Remove previous 'tg.exe' executable (if present)
if (Test-Path -Path (Join-Path $Erigon.BinPath "tg.exe") -PathType Leaf) {
    Remove-Item -Path (Join-Path $Erigon.BinPath "tg.exe")
}

Write-Host " Building Erigon ..."
$outExecutable = [string](Join-Path $Erigon.BinPath "erigon.exe")
$BuildCommand = "$($Erigon.Build) -o ""$($outExecutable)"" ./cmd/erigon"
$BuildCommand += ';$?'
$success = Invoke-Expression -Command $BuildCommand
if (-not $success) {
    Write-Host "Could not build Erigon executable"
    return
}

Write-Host " Building rpcdaemon ..."
$outExecutable = [string](Join-Path $Erigon.BinPath "rpcdaemon.exe")
$BuildCommand = "$($Erigon.Build) -o ""$($outExecutable)"" ./cmd/rpcdaemon"
$BuildCommand += ';$?'
$success = Invoke-Expression -Command $BuildCommand
if (-not $success) {
    Write-Host "Could not build rpcdaemon executable"
    return
}

Write-Host " Building integration ..."
$outExecutable = [string](Join-Path $Erigon.BinPath "integration.exe")
$BuildCommand = "$($Erigon.Build) -o ""$($outExecutable)"" ./cmd/integration"
$BuildCommand += ';$?'
$success = Invoke-Expression -Command $BuildCommand
if (-not $success) {
    Write-Host "Could not build integration executable"
    return
}

Write-Host " Building rpctest ..."
$outExecutable = [string](Join-Path $Erigon.BinPath "rpctest.exe")
$BuildCommand = "$($Erigon.Build) -o ""$($outExecutable)"" ./cmd/rpctest"
$BuildCommand += ';$?'
$success = Invoke-Expression -Command $BuildCommand
if (-not $success) {
    Write-Host "Could not build rpctest executable"
    return
}

Write-Host " Building state ..."
$outExecutable = [string](Join-Path $Erigon.BinPath "state.exe")
$BuildCommand = "$($Erigon.Build) -o ""$($outExecutable)"" ./cmd/state"
$BuildCommand += ';$?'
$success = Invoke-Expression -Command $BuildCommand
if (-not $success) {
    Write-Host "Could not build state executable"
    return
}
