#!/usr/bin/env bash

make evm

# File to store the output of failed tests
failed_tests_output="failed_tests.log"

# Clear the log file if it exists
> "$failed_tests_output"


osaka_path=./tests/osaka-eof/state_tests 


# Function to run state tests
run_state_tests() {

    if [ "$#" -eq 0 ]; then
        echo "Error: No paths provided to run_state_tests"
        echo "Usage: $0 run_state_tests <path1> <path2> ..."
        return 1
    fi

    for test_path in "$@"; do
        if [ -d "$test_path" ]; then
            echo "Running tests in directory: $test_path"
            for file in $(find "$test_path" -type f -name "*.json"); do
                # Run the test and capture the output
                ./build/bin/evm statetest "$file" > test_output.log 2>&1

                # Check for "pass": false in the output
                if grep -q '"pass": false' test_output.log; then
                    echo "Test failed for: $file"
                    cat test_output.log
                    echo "Test failed for: $file" >> "$failed_tests_output"
                    cat test_output.log >> "$failed_tests_output" # Append the test output to the log file
                    echo "----------------------------------------" >> "$failed_tests_output"
                fi
            done
        else
            echo "Directory not found: $test_path"
        fi
    done

    # Clean up temporary test output file
    rm -f test_output.log
}

# Function to run diff fuzz
run_diff_fuzz() { # not fully implemented yet
    # The following steps should be prepared in advance:
    # https://hackmd.io/@shemnon/eof-diff-fuzz

    # expects at leaset 4 parameters
    # 1. directory where state_tests are located (e.g. $HOME/execution-spec-tests/fuzzing/state_tests)
    # 2. goevmlab runtest executable (e.g. $HOME/goevmlab/runtest)
    # 3. erigon evm executable (e.g. $HOME/eof_erigon/erigon/build/bin/evm)
    # 4. besu evm executable (e.g. $HOME/besu-25.3.0/bin/evmtool)

    # Ensure at least 4 parameters are provided
    if [ "$#" -lt 4 ]; then
        echo "Error: run_diff_fuzz expects at least 4 parameters"
        echo "Usage: $0 run_diff_fuzz <statetest_dir> <runtest_executable> <client1> <client2> [additional_args...]"
        return 1
    fi

    # Assign the first 5 parameters to variables
    local shemnon_fork=$1
    local statetest_dir=$2
    local runtest_executable=$3
    local client1=$4
    local client2=$5

    local statetest_dir=${1:-"$HOME/execution-spec-tests/fuzzing/state_tests"} # Default fuzzing/state_tests
    local runtest_executable=${2:-"$HOME/goevmlab/runtest"} # Default to goevmlab runtest
    local client1=${3:-"$HOME/eof_erigon/erigon/build/bin/evm"} # Default to erigon evm
    local client2=${4:-"$HOME/besu-25.3.0/bin/evmtool"} # Default to besu evmtool

    # Print warnings if default values are used
    if [ -z "$1" ]; then
        echo "Warning: Using default value for State Test Directory: $statetest_dir"
    fi
    if [ -z "$2" ]; then
        echo "Warning: Using default value for RunTest Executable: $runtest_executable"
    fi
    if [ -z "$3" ]; then
        echo "Warning: Using default value for Client 1: $client1"
    fi
    if [ -z "$4" ]; then
        echo "Warning: Using default value for Client 2: $client2"
    fi

    # Shift the first 4 arguments to handle any additional arguments
    shift 4

    echo "Running diff fuzz with the following parameters:"
    echo "State Test Directory: $statetest_dir"
    echo "RunTest Executable: $runtest_executable"
    echo "Client 1: $client1"
    echo "Client 2: $client2"
    echo "Additional Arguments: $@"

    # Print additional arguments
    if [ "$#" -gt 0 ]; then
        echo "Additional Arguments: $@"
    else
        echo "No additional arguments were passed."
    fi

    cd $shemnon_fork 
    git checkout shemnon/eof-fuzz
    git pull
    # Run the diff fuzz command
    uv run diff_fuzz \
        -w /tmp/diff_fuzz \
        -c "$statetest_dir" \
        --cleanup-tests True \
        -r "$runtest_executable" \
        --client erigon "$client1" \
        --client besubatch "$client2" \
        --skip-trace False \
        --step-count 1000 \
        --step-num 1 \
        --max-gas 100000000 \
        "$@" # add additional arguments here (e.g another clients)
}


# Check the first argument to determine which function to execute
if [ "$1" == "run_state_tests" ]; then
    shift # Remove the first argument so the remaining arguments can be passed to the function
    run_state_tests "$@"
elif [ "$1" == "run_diff_fuzz" ]; then
    shift 
    run_diff_fuzz "$@"
else
    echo "Usage: $0 {run_state_tests|run_diff_fuzz} [arguments...]"
    exit 1
fi

# run_state_tests "$osaka_path" # Add more paths as needed

