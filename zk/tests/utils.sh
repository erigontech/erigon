#!/bin/bash

run() {
    local func_name=$1
    shift  # shift off the function name, leave only the arguments
    echo "--------------- Starting $func_name ---------------"
    $func_name "$@"
    local result=$?

    if [ $result -ne 0 ]; then
        echo "--------------- $func_name failed with exit code $result ---------------"
        exit $result
    else
        echo "--------------- Completed $func_name ---------------"
    fi
    return $result
}
