# wmake.ps1 - Thin wrapper that delegates to Makefile via bash
# Requires: Git for Windows (provides bash), GNU Make (choco install make)
$ErrorActionPreference = "Stop"

$bash = Get-Command bash -ErrorAction SilentlyContinue
if (-not $bash) {
    Write-Error "bash not found. Install Git for Windows: https://git-scm.com/downloads"
    exit 1
}

# Build the make command - default target is 'all' to match old wmake.ps1 behavior
$makeArgs = if ($args.Count -gt 0) { $args -join " " } else { "all" }

# Run through bash to ensure MSYS2 environment (Unix utilities, /bin/bash, etc.)
& bash -c "make $makeArgs"
exit $LASTEXITCODE
