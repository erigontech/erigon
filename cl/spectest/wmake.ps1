# Clean Function
function Clean {
    Remove-Item -Recurse -Force -Path .\tests
}

# Tests Function
function Tests {
    $env:GIT_LFS_SKIP_SMUDGE = "1"
    $gitCloneCmd =    "git clone https://github.com/ethereum/consensus-spec-tests"
    $gitCheckoutCmd = "cd consensus-spec-tests; git checkout 70dc28b18c71f3ae080c02f51bd3421e0b60609b; git lfs pull --exclude=tests/general,tests/minimal; cd .."
    
    Invoke-Expression $gitCloneCmd
    Invoke-Expression $gitCheckoutCmd

    Move-Item -Path ".\consensus-spec-tests\tests" -Destination ".\" -Force
    Remove-Item -Path ".\consensus-spec-tests" -Recurse -Force
    Remove-Item -Path ".\tests\minimal" -Recurse -Force
    Remove-Item -Path ".\tests\mainnet\eip6110" -Recurse -Force
    Remove-Item -Path ".\tests\mainnet\deneb" -Recurse -Force
}

# Mainnet Function
function Mainnet {
    $env:CGO_CFLAGS = "-D__BLST_PORTABLE__"
    go test -tags=spectest -run="/mainnet" -failfast -v
}

# Main Targets
if ($MyInvocation.BoundParameters["clean"]) {
    Clean
}
elseif ($MyInvocation.BoundParameters["tests"]) {
    Tests
} else {
    Mainnet
}

