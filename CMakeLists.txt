cmake_minimum_required(VERSION 3.27)

include(FetchContent)

FetchContent_Declare(
    evmone
    GIT_REPOSITORY https://github.com/racytech/evmone.git
)

FetchContent_Declare(
    intx
    GIT_REPOSITORY https://github.com/chfast/intx.git
    GIT_TAG v0.10.1
)

FetchContent_MakeAvailable(evmone intx)

project(evmone-go LANGUAGES C CXX)