cmake_minimum_required(VERSION 3.27)

include(FetchContent)

FetchContent_Declare(
    evmone
    GIT_REPOSITORY https://github.com/ethereum/evmone.git
)
FetchContent_MakeAvailable(evmone)

project(evmone-go LANGUAGES C CXX)