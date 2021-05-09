##  Copyright (c) 2012-2021 Leonid Yuriev <leo@yuriev.ru>.
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##

cmake_minimum_required(VERSION 3.8.2)
cmake_policy(PUSH)
cmake_policy(VERSION 3.8.2)

if(CMAKE_VERSION MATCHES ".*MSVC.*" AND CMAKE_VERSION VERSION_LESS 3.16)
  message(FATAL_ERROR "CMake from MSVC kit is unfit! "
    "Please use MSVC2019 with modern CMake the original CMake from https://cmake.org/download/")
endif()

if(NOT (CMAKE_C_COMPILER_LOADED OR CMAKE_CXX_COMPILER_LOADED))
  message(FATAL_ERROR "This module required C or C++ to be enabled")
endif()

if(CMAKE_CXX_COMPILER_LOADED)
  include(CheckCXXSourceRuns)
  include(CheckCXXSourceCompiles)
  include(CheckCXXCompilerFlag)
endif()
if(CMAKE_C_COMPILER_LOADED)
  include(CheckCSourceRuns)
  include(CheckCSourceCompiles)
  include(CheckCCompilerFlag)
endif()
include(CMakeDependentOption)
include(CheckLibraryExists)
include(CheckIncludeFiles)

# Check if the same compile family is used for both C and CXX
if(CMAKE_C_COMPILER_LOADED AND CMAKE_CXX_COMPILER_LOADED AND
    NOT (CMAKE_C_COMPILER_ID STREQUAL CMAKE_CXX_COMPILER_ID))
  message(WARNING "CMAKE_C_COMPILER_ID (${CMAKE_C_COMPILER_ID}) is different "
    "from CMAKE_CXX_COMPILER_ID (${CMAKE_CXX_COMPILER_ID}). "
    "The final binary may be unusable.")
endif()

if(CMAKE_CXX_COMPILER_LOADED)
  set(CMAKE_PRIMARY_LANG "CXX")
else()
  set(CMAKE_PRIMARY_LANG "C")
endif()

macro(check_compiler_flag flag variable)
  if(CMAKE_CXX_COMPILER_LOADED)
    check_cxx_compiler_flag(${flag} ${variable})
  else()
    check_c_compiler_flag(${flag} ${variable})
  endif()
endmacro(check_compiler_flag)

# We support building with Clang and gcc. First check
# what we're using for build.
if(CMAKE_C_COMPILER_LOADED AND CMAKE_C_COMPILER_ID MATCHES ".*[Cc][Ll][Aa][Nn][Gg].*")
  set(CMAKE_COMPILER_IS_CLANG  ON)
  set(CMAKE_COMPILER_IS_GNUCC  OFF)
endif()
if(CMAKE_CXX_COMPILER_LOADED AND CMAKE_CXX_COMPILER_ID MATCHES ".*[Cc][Ll][Aa][Nn][Gg].*")
  set(CMAKE_COMPILER_IS_CLANG  ON)
  set(CMAKE_COMPILER_IS_GNUCXX OFF)
endif()

if(CMAKE_C_COMPILER_LOADED)
  # Check for Elbrus lcc
  execute_process(COMMAND ${CMAKE_C_COMPILER} --version
    OUTPUT_VARIABLE tmp_lcc_probe_version
    RESULT_VARIABLE tmp_lcc_probe_result ERROR_QUIET)
  if(tmp_lcc_probe_result EQUAL 0)
    string(FIND "${tmp_lcc_probe_version}" "lcc:" tmp_lcc_marker)
    string(FIND "${tmp_lcc_probe_version}" ":e2k-" tmp_e2k_marker)
    if(tmp_lcc_marker GREATER -1 AND tmp_e2k_marker GREATER tmp_lcc_marker)
      execute_process(COMMAND ${CMAKE_C_COMPILER} -print-version
        OUTPUT_VARIABLE CMAKE_C_COMPILER_VERSION
        RESULT_VARIABLE tmp_lcc_probe_result)
      set(CMAKE_COMPILER_IS_ELBRUSC ON)
      set(CMAKE_C_COMPILER_ID "Elbrus")
    else()
      set(CMAKE_COMPILER_IS_ELBRUSC OFF)
    endif()
    unset(tmp_lcc_marker)
    unset(tmp_e2k_marker)
  endif()
  unset(tmp_lcc_probe_version)
  unset(tmp_lcc_probe_result)
endif()

if(CMAKE_CXX_COMPILER_LOADED)
  # Check for Elbrus l++
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version
    OUTPUT_VARIABLE tmp_lxx_probe_version
    RESULT_VARIABLE tmp_lxx_probe_result ERROR_QUIET)
  if(tmp_lxx_probe_result EQUAL 0)
    string(FIND "${tmp_lxx_probe_version}" "lcc:" tmp_lcc_marker)
    string(FIND "${tmp_lxx_probe_version}" ":e2k-" tmp_e2k_marker)
    if(tmp_lcc_marker GREATER -1 AND tmp_e2k_marker GREATER tmp_lcc_marker)
      execute_process(COMMAND ${CMAKE_CXX_COMPILER} -print-version
        OUTPUT_VARIABLE CMAKE_CXX_COMPILER_VERSION
        RESULT_VARIABLE tmp_lxx_probe_result)
      set(CMAKE_COMPILER_IS_ELBRUSCXX ON)
      set(CMAKE_CXX_COMPILER_ID "Elbrus")
    else()
      set(CMAKE_COMPILER_IS_ELBRUSCXX OFF)
    endif()
    unset(tmp_lcc_marker)
    unset(tmp_e2k_marker)
  endif()
  unset(tmp_lxx_probe_version)
  unset(tmp_lxx_probe_result)
endif()

# Hard coding the compiler version is ugly from cmake POV, but
# at least gives user a friendly error message. The most critical
# demand for C++ compiler is support of C++11 lambdas, added
# only in version 4.5 https://gcc.gnu.org/projects/cxx0x.html
if(CMAKE_COMPILER_IS_GNUCC)
  if(CMAKE_C_COMPILER_VERSION VERSION_LESS 4.5
      AND NOT CMAKE_COMPILER_IS_ELBRUSC)
    message(FATAL_ERROR "
      Your GCC version is ${CMAKE_C_COMPILER_VERSION}, please update")
  endif()
endif()
if(CMAKE_COMPILER_IS_GNUCXX)
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.5
      AND NOT CMAKE_COMPILER_IS_ELBRUSCXX)
    message(FATAL_ERROR "
      Your G++ version is ${CMAKE_CXX_COMPILER_VERSION}, please update")
  endif()
endif()

if(CMAKE_CL_64)
  set(MSVC64 1)
endif()
if(WIN32 AND CMAKE_COMPILER_IS_GNU${CMAKE_PRIMARY_LANG})
  execute_process(COMMAND ${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER} -dumpmachine
    OUTPUT_VARIABLE __GCC_TARGET_MACHINE
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(__GCC_TARGET_MACHINE MATCHES "amd64|x86_64|AMD64")
    set(MINGW64 1)
  endif()
  unset(__GCC_TARGET_MACHINE)
endif()

if(NOT DEFINED IOS)
  if(APPLE AND (CMAKE_SYSTEM_NAME STREQUAL "iOS"
      OR DEFINED CMAKE_IOS_DEVELOPER_ROOT
      OR DEFINED IOS_PLATFORM OR DEFINED IOS_ARCH))
    set(IOS TRUE)
  else()
    set(IOS FALSE)
  endif()
endif()

if(NOT DEFINED CMAKE_TARGET_BITNESS)
  if (CMAKE_SIZEOF_VOID_P LESS 4)
    set(CMAKE_TARGET_BITNESS 16)
  elseif (CMAKE_SIZEOF_VOID_P LESS 8)
    set(CMAKE_TARGET_BITNESS 32)
  else()
    set(CMAKE_TARGET_BITNESS 64)
  endif()
endif()

if(CMAKE_COMPILER_IS_ELBRUSC OR CMAKE_SYSTEM_PROCESSOR MATCHES "e2k.*|E2K.*|elbrus.*|ELBRUS.*")
  set(E2K TRUE)
  set(CMAKE_SYSTEM_ARCH "Elbrus")
elseif((MSVC64 OR MINGW64) AND CMAKE_TARGET_BITNESS EQUAL 64)
  set(X86_64 TRUE)
  set(CMAKE_SYSTEM_ARCH "x86_64")
elseif(MINGW OR (MSVC AND NOT CMAKE_CROSSCOMPILING))
  set(X86_32 TRUE)
  set(CMAKE_SYSTEM_ARCH "x86")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64.*|x86_64.*|AMD64.*" AND CMAKE_TARGET_BITNESS EQUAL 64)
  set(X86_64 TRUE)
  set(CMAKE_SYSTEM_ARCH "x86_64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "i686.*|i386.*|x86.*")
  set(X86_32 TRUE)
  set(CMAKE_SYSTEM_ARCH "x86")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|ARM64.*)" AND CMAKE_TARGET_BITNESS EQUAL 64)
  set(AARCH64 TRUE)
  set(CMAKE_SYSTEM_ARCH "ARM64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm.*|ARM.*)")
  set(ARM32 TRUE)
  set(CMAKE_SYSTEM_ARCH "ARM")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64le.*" AND CMAKE_TARGET_BITNESS EQUAL 64)
  set(PPC64LE TRUE)
  set(CMAKE_SYSTEM_ARCH "PPC64LE")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64.*" AND CMAKE_TARGET_BITNESS EQUAL 64)
  set(PPC64 TRUE)
  set(CMAKE_SYSTEM_ARCH "PPC64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc).*")
  set(PPC32 TRUE)
  set(CMAKE_SYSTEM_ARCH "PPC")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(mips|MIPS)64.*" AND CMAKE_TARGET_BITNESS EQUAL 64)
  set(MIPS64 TRUE)
  set(CMAKE_SYSTEM_ARCH "MIPS64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(mips|MIPS).*")
  set(MIPS32 TRUE)
  set(CMAKE_SYSTEM_ARCH "MIPS")
endif()

if(MSVC)
  check_compiler_flag("/WX" CC_HAS_WERROR)
else()
  #
  # GCC started to warn for unused result starting from 4.2, and
  # this is when it introduced -Wno-unused-result
  # GCC can also be built on top of llvm runtime (on mac).
  check_compiler_flag("-Wno-unknown-pragmas" CC_HAS_WNO_UNKNOWN_PRAGMAS)
  check_compiler_flag("-Wextra" CC_HAS_WEXTRA)
  check_compiler_flag("-Werror" CC_HAS_WERROR)
  check_compiler_flag("-fexceptions" CC_HAS_FEXCEPTIONS)
  if(CMAKE_CXX_COMPILER_LOADED)
    check_cxx_compiler_flag("-fcxx-exceptions" CC_HAS_FCXX_EXCEPTIONS)
  endif()
  check_compiler_flag("-funwind-tables" CC_HAS_FUNWIND_TABLES)
  check_compiler_flag("-fno-omit-frame-pointer" CC_HAS_FNO_OMIT_FRAME_POINTER)
  check_compiler_flag("-fno-common" CC_HAS_FNO_COMMON)
  check_compiler_flag("-ggdb" CC_HAS_GGDB)
  check_compiler_flag("-fvisibility=hidden" CC_HAS_VISIBILITY)
  check_compiler_flag("-march=native" CC_HAS_ARCH_NATIVE)
  check_compiler_flag("-Og" CC_HAS_DEBUG_FRIENDLY_OPTIMIZATION)
  check_compiler_flag("-Wall" CC_HAS_WALL)
  check_compiler_flag("-Ominimal" CC_HAS_OMINIMAL)
  check_compiler_flag("-ffunction-sections -fdata-sections" CC_HAS_SECTIONS)
  check_compiler_flag("-ffast-math" CC_HAS_FASTMATH)
  check_compiler_flag("-Wno-attributes" CC_HAS_WNO_ATTRIBUTES)

  # Check for an omp support
  set(CMAKE_REQUIRED_FLAGS "-fopenmp -Werror")
  if(CMAKE_CXX_COMPILER_LOADED)
    check_cxx_source_compiles("int main(void) {
      #pragma omp parallel
      return 0;
      }" HAVE_OPENMP)
  else()
    check_c_source_compiles("int main(void) {
      #pragma omp parallel
      return 0;
      }" HAVE_OPENMP)
  endif()
  set(CMAKE_REQUIRED_FLAGS "")
endif()

# Crutch for old C++ compilers and/or CMake to enabling C++11
if(CMAKE_CXX_COMPILER_LOADED)
  list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_11 HAS_CXX11)
  if(HAS_CXX11 LESS 0)
    check_compiler_flag("-std=gnu++11" CXX_FALLBACK_STDGNU11)
    if(NOT CXX_FALLBACK_STDGNU11)
      check_compiler_flag("-std=c++11" CXX_FALLBACK_STD11)
    endif()
  endif()
endif()

# Check for LTO support by GCC
if(CMAKE_COMPILER_IS_GNU${CMAKE_PRIMARY_LANG})
  unset(gcc_collect)
  unset(gcc_lto_wrapper)

  if(NOT CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 4.7)
    execute_process(COMMAND ${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER} -v
      OUTPUT_VARIABLE gcc_info_v ERROR_VARIABLE gcc_info_v)

    string(REGEX MATCH "^(.+\nCOLLECT_GCC=)([^ \n]+)(\n.+)$" gcc_collect_valid ${gcc_info_v})
    if(gcc_collect_valid)
      string(REGEX REPLACE "^(.+\nCOLLECT_GCC=)([^ \n]+)(\n.+)$" "\\2" gcc_collect ${gcc_info_v})
    endif()

    string(REGEX MATCH "^(.+\nCOLLECT_LTO_WRAPPER=)([^ \n]+/lto-wrapper)(\n.+)$" gcc_lto_wrapper_valid ${gcc_info_v})
    if(gcc_lto_wrapper_valid)
      string(REGEX REPLACE "^(.+\nCOLLECT_LTO_WRAPPER=)([^ \n]+/lto-wrapper)(\n.+)$" "\\2" gcc_lto_wrapper ${gcc_info_v})
    endif()

    set(gcc_suffix "")
    if(gcc_collect_valid AND gcc_collect)
      string(REGEX MATCH "^(.*cc)(-.+)$" gcc_suffix_valid ${gcc_collect})
      if(gcc_suffix_valid)
        string(REGEX MATCH "^(.*cc)(-.+)$" "\\2" gcc_suffix ${gcc_collect})
      endif()
    endif()

    get_filename_component(gcc_dir ${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER} DIRECTORY)
    if(NOT CMAKE_GCC_AR)
      find_program(CMAKE_GCC_AR NAMES gcc${gcc_suffix}-ar gcc-ar${gcc_suffix} PATHS ${gcc_dir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_GCC_NM)
      find_program(CMAKE_GCC_NM NAMES gcc${gcc_suffix}-nm gcc-nm${gcc_suffix} PATHS ${gcc_dir} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_GCC_RANLIB)
      find_program(CMAKE_GCC_RANLIB NAMES gcc${gcc_suffix}-ranlib gcc-ranlib${gcc_suffix} PATHS ${gcc_dir} NO_DEFAULT_PATH)
    endif()

    unset(gcc_dir)
    unset(gcc_suffix_valid)
    unset(gcc_suffix)
    unset(gcc_lto_wrapper_valid)
    unset(gcc_collect_valid)
    unset(gcc_collect)
    unset(gcc_info_v)
  endif()

  if(CMAKE_GCC_AR AND CMAKE_GCC_NM AND CMAKE_GCC_RANLIB AND gcc_lto_wrapper)
    message(STATUS "Found GCC's LTO toolset: ${gcc_lto_wrapper}, ${CMAKE_GCC_AR}, ${CMAKE_GCC_RANLIB}")
    set(GCC_LTO_CFLAGS "-flto -fno-fat-lto-objects -fuse-linker-plugin")
    set(GCC_LTO_AVAILABLE TRUE)
    message(STATUS "Link-Time Optimization by GCC is available")
  else()
    set(GCC_LTO_AVAILABLE FALSE)
    message(STATUS "Link-Time Optimization by GCC is NOT available")
  endif()
  unset(gcc_lto_wrapper)
endif()

# check for LTO by MSVC
if(MSVC AND NOT CMAKE_COMPILER_IS_CLANG)
  if(NOT MSVC_VERSION LESS 1600)
    set(MSVC_LTO_AVAILABLE TRUE)
    message(STATUS "Link-Time Optimization by MSVC is available")
  else()
    set(MSVC_LTO_AVAILABLE FALSE)
    message(STATUS "Link-Time Optimization by MSVC is NOT available")
  endif()
endif()

# Check for LTO support by CLANG
if(CMAKE_COMPILER_IS_CLANG)
  if(NOT CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 3.5)
    execute_process(COMMAND ${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER} -print-search-dirs
      OUTPUT_VARIABLE clang_search_dirs RESULT_VARIABLE clang_probe_result ERROR_QUIET)

    unset(clang_bindirs)
    unset(clang_libdirs)
    if(clang_probe_result EQUAL 0)
        string(REGEX MATCH "(^|\n.*)(.*programs: =)([^\n]+)((\n.*)|$)" regexp_valid ${clang_search_dirs})
        if(regexp_valid)
          string(REGEX REPLACE "(^|\n.*)(.*programs: =)([^\n]+)((\n.*)|$)" "\\3" list ${clang_search_dirs})
          string(REPLACE ":" ";" list "${list}")
          #set(clang_bindirs "")
          foreach(dir IN LISTS list)
            get_filename_component(dir "${dir}" REALPATH)
            list(APPEND clang_bindirs "${dir}")
          endforeach()
          list(REMOVE_DUPLICATES clang_bindirs)
        endif()
        string(REGEX MATCH "(^|\n.*)(.*libraries: =)([^\n]+)((\n.*)|$)" regexp_valid ${clang_search_dirs})
        if(regexp_valid)
          string(REGEX REPLACE "(^|\n.*)(.*libraries: =)([^\n]+)((\n.*)|$)" "\\3" list ${clang_search_dirs})
          string(REPLACE ":" ";" list "${list}")
          #set(clang_libdirs "")
          foreach(dir IN LISTS list)
            get_filename_component(dir "${dir}" REALPATH)
            list(APPEND clang_libdirs "${dir}")
          endforeach()
          list(REMOVE_DUPLICATES clang_libdirs)
        endif()
    else()
      get_filename_component(clang_bindirs ${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER} DIRECTORY)
      if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Windows")
        set(clang_libdirs ${clang_bindirs})
      else()
        get_filename_component(clang_libdirs "${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER}/../lib" REALPATH)
      endif()
    endif()

    if(clang_bindirs AND clang_libdirs)
      message(STATUS "Found CLANG/LLVM directories: ${clang_bindirs}, ${clang_libdirs}")
    else()
      message(STATUS "Could NOT find CLANG/LLVM directories (bin and/or lib).")
    endif()

    if(NOT CMAKE_CLANG_LD AND clang_bindirs)
      find_program(CMAKE_CLANG_LD NAMES lld-link ld.lld "ld${CMAKE_TARGET_BITNESS}.lld" lld llvm-link llvm-ld PATHS ${clang_bindirs} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_CLANG_AR AND clang_bindirs)
      find_program(CMAKE_CLANG_AR NAMES llvm-ar ar PATHS ${clang_bindirs} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_CLANG_NM AND clang_bindirs)
      find_program(CMAKE_CLANG_NM NAMES llvm-nm nm PATHS ${clang_bindirs} NO_DEFAULT_PATH)
    endif()
    if(NOT CMAKE_CLANG_RANLIB AND clang_bindirs)
      find_program(CMAKE_CLANG_RANLIB NAMES llvm-ranlib ranlib PATHS ${clang_bindirs} NO_DEFAULT_PATH)
    endif()

    set(clang_lto_plugin_name "LLVMgold${CMAKE_SHARED_LIBRARY_SUFFIX}")
    if(NOT CMAKE_LD_GOLD AND clang_bindirs)
      find_program(CMAKE_LD_GOLD NAMES ld.gold PATHS ${clang_bindirs})
    endif()
    if(NOT CLANG_LTO_PLUGIN AND clang_libdirs)
      find_file(CLANG_LTO_PLUGIN ${clang_lto_plugin_name} PATHS ${clang_libdirs} NO_DEFAULT_PATH)
    endif()

    if(CLANG_LTO_PLUGIN)
      message(STATUS "Found CLANG/LLVM's plugin for LTO: ${CLANG_LTO_PLUGIN}")
    else()
      message(STATUS "Could NOT find CLANG/LLVM's plugin (${clang_lto_plugin_name}) for LTO.")
    endif()

    if(CMAKE_CLANG_LD)
      message(STATUS "Found CLANG/LLVM's linker for LTO: ${CMAKE_CLANG_LD}")
    else()
      message(STATUS "Could NOT find CLANG/LLVM's linker (lld, llvm-ld, llvm-link) for LTO.")
    endif()

    if(CMAKE_CLANG_AR AND CMAKE_CLANG_RANLIB AND CMAKE_CLANG_NM)
      message(STATUS "Found CLANG/LLVM's binutils for LTO: ${CMAKE_CLANG_AR}, ${CMAKE_CLANG_RANLIB}, ${CMAKE_CLANG_NM}")
    else()
      message(STATUS "Could NOT find CLANG/LLVM's binutils (ar, ranlib, nm) for LTO.")
    endif()

    unset(clang_lto_plugin_name)
    unset(clang_libdir)
    unset(clang_bindir_valid)
    unset(clang_bindir)
    unset(clang_search_dirs)
  endif()

  if (CMAKE_CLANG_AR AND CMAKE_CLANG_NM AND CMAKE_CLANG_RANLIB
      AND ((CLANG_LTO_PLUGIN AND CMAKE_LD_GOLD)
        OR (CMAKE_CLANG_LD
          AND NOT (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux"
            AND CMAKE_SYSTEM_NAME STREQUAL "Linux"))
        OR APPLE))
    set(CLANG_LTO_AVAILABLE TRUE)
    message(STATUS "Link-Time Optimization by CLANG/LLVM is available")
  elseif(CMAKE_TOOLCHAIN_FILE AND NOT CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 7.0)
    set(CLANG_LTO_AVAILABLE TRUE)
    if(NOT CMAKE_CLANG_LD)
      set(CMAKE_CLANG_LD ${CMAKE_LINKER})
    endif()
    if(NOT CMAKE_CLANG_AR)
      set(CMAKE_CLANG_AR ${CMAKE_AR})
    endif()
    if(NOT CMAKE_CLANG_NM)
      set(CMAKE_CLANG_NM ${CMAKE_NM})
    endif()
    if(NOT CMAKE_CLANG_RANLIB)
      set(CMAKE_CLANG_RANLIB ${CMAKE_RANLIB})
    endif()
    message(STATUS "Assume Link-Time Optimization by CLANG/LLVM is available via ${CMAKE_TOOLCHAIN_FILE}")
  else()
    set(CLANG_LTO_AVAILABLE FALSE)
    message(STATUS "Link-Time Optimization by CLANG/LLVM is NOT available")
  endif()
endif()

# Perform build type specific configuration.
option(ENABLE_BACKTRACE "Enable output of fiber backtrace information in 'show
  fiber' administrative command. Only works on x86 architectures, if compiled
  with gcc. If GNU binutils and binutils-dev libraries are installed, backtrace
  is output with resolved function (symbol) names. Otherwise only frame
  addresses are printed." OFF)

set(HAVE_BFD False)
if(ENABLE_BACKTRACE)
  if(NOT (X86_32 OR X86_64) OR NOT CMAKE_COMPILER_IS_GNU${CMAKE_PRIMARY_LANG})
    # We only know this option to work with gcc
    message(FATAL_ERROR "ENABLE_BACKTRACE option is set but the system
      is not x86 based (${CMAKE_SYSTEM_PROCESSOR}) or the compiler
      is not GNU GCC (${CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER}).")
  endif()
  # Use GNU bfd if present.
  find_library(BFD_LIBRARY NAMES libbfd.a)
  if(BFD_LIBRARY)
    check_library_exists(${BFD_LIBRARY} bfd_init "" HAVE_BFD_LIB)
  endif()
  find_library(IBERTY_LIBRARY NAMES libiberty.a)
  if(IBERTY_LIBRARY)
    check_library_exists(${IBERTY_LIBRARY} cplus_demangle "" HAVE_IBERTY_LIB)
  endif()
  set(CMAKE_REQUIRED_DEFINITIONS -DPACKAGE=${PACKAGE} -DPACKAGE_VERSION=${PACKAGE_VERSION})
  check_include_files(bfd.h HAVE_BFD_H)
  set(CMAKE_REQUIRED_DEFINITIONS)
  find_package(ZLIB)
  if(HAVE_BFD_LIB AND HAVE_BFD_H AND HAVE_IBERTY_LIB AND ZLIB_FOUND)
    set(HAVE_BFD ON)
    set(BFD_LIBRARIES ${BFD_LIBRARY} ${IBERTY_LIBRARY} ${ZLIB_LIBRARIES})
    find_package_message(BFD_LIBRARIES "Found libbfd and dependencies"
      ${BFD_LIBRARIES})
    if(TARGET_OS_FREEBSD AND NOT TARGET_OS_DEBIAN_FREEBSD)
      set(BFD_LIBRARIES ${BFD_LIBRARIES} iconv)
    endif()
  endif()
endif()

macro(setup_compile_flags)
  # save initial C/CXX flags
  if(NOT INITIAL_CMAKE_FLAGS_SAVED)
    if(CMAKE_CXX_COMPILER_LOADED)
      set(INITIAL_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    endif()
    if(CMAKE_C_COMPILER_LOADED)
      set(INITIAL_CMAKE_C_FLAGS ${CMAKE_C_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    endif()
    set(INITIAL_CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_SHARED_LINKER_FLAGS ${CMAKE_SHARED_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_STATIC_LINKER_FLAGS ${CMAKE_STATIC_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_MODULE_LINKER_FLAGS ${CMAKE_MODULE_LINKER_FLAGS} CACHE STRING "Initial CMake's flags" FORCE)
    set(INITIAL_CMAKE_FLAGS_SAVED TRUE CACHE INTERNAL "State of initial CMake's flags" FORCE)
  endif()

  # reset C/CXX flags
  if(CMAKE_CXX_COMPILER_LOADED)
    set(CXX_FLAGS ${INITIAL_CMAKE_CXX_FLAGS})
    # Crutch for old C++ compilers and/or CMake to enabling C++11
    if(CXX_FALLBACK_STDGNU11)
      add_compile_flags("CXX" "-std=gnu++11")
    elseif(CXX_FALLBACK_STD11)
      add_compile_flags("CXX" "-std=c++11")
    endif()
  endif()
  if(CMAKE_C_COMPILER_LOADED)
    set(C_FLAGS ${INITIAL_CMAKE_C_FLAGS})
  endif()
  set(EXE_LINKER_FLAGS ${INITIAL_CMAKE_EXE_LINKER_FLAGS})
  set(SHARED_LINKER_FLAGS ${INITIAL_CMAKE_SHARED_LINKER_FLAGS})
  set(STATIC_LINKER_FLAGS ${INITIAL_CMAKE_STATIC_LINKER_FLAGS})
  set(MODULE_LINKER_FLAGS ${INITIAL_CMAKE_MODULE_LINKER_FLAGS})

  if(CC_HAS_FEXCEPTIONS)
    add_compile_flags("C;CXX" "-fexceptions")
  endif()
  if(CC_HAS_FCXX_EXCEPTIONS)
    add_compile_flags("CXX" "-fcxx-exceptions" "-frtti")
  endif()
  if(MSVC)
    # checks for /EHa or /clr options exists,
    # i.e. is enabled structured async WinNT exceptions
    string(REGEX MATCH "^(.* )*[-/]EHc*a( .*)*$" msvc_async_eh_enabled "${CXX_FLAGS}" "${C_FLAGS}")
    string(REGEX MATCH "^(.* )*[-/]clr( .*)*$" msvc_clr_enabled "${CXX_FLAGS}" "${C_FLAGS}")
    # remote any /EH? options
    string(REGEX REPLACE "( *[-/]-*EH[csa]+ *)+" "" CXX_FLAGS "${CXX_FLAGS}")
    string(REGEX REPLACE "( *[-/]-*EH[csa]+ *)+" "" C_FLAGS "${C_FLAGS}")
    if (msvc_clr_enabled STREQUAL "")
      if(NOT msvc_async_eh_enabled STREQUAL "")
        add_compile_flags("C;CXX" "/EHa")
      else()
        add_compile_flags("C;CXX" "/EHsc")
      endif()
    endif()
  endif(MSVC)

  if(CC_HAS_WNO_ATTRIBUTES AND CMAKE_COMPILER_IS_GNU${CMAKE_PRIMARY_LANG}
      AND CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 9)
    # GCC < 9.x generates false-positive warnings for optimization attributes
    add_compile_flags("C;CXX" "-Wno-attributes")
  endif()

  # In C a global variable without a storage specifier (static/extern) and
  # without an initialiser is called a ’tentative definition’. The
  # language permits multiple tentative definitions in the single
  # translation unit; i.e. int foo; int foo; is perfectly ok. GNU
  # toolchain goes even further, allowing multiple tentative definitions
  # in *different* translation units. Internally, variables introduced via
  # tentative definitions are implemented as ‘common’ symbols. Linker
  # permits multiple definitions if they are common symbols, and it picks
  # one arbitrarily for inclusion in the binary being linked.
  #
  # -fno-common forces GNU toolchain to behave in a more
  # standard-conformant way in respect to tentative definitions and it
  # prevents common symbols generation. Since we are a cross-platform
  # project it really makes sense. There are toolchains that don’t
  # implement GNU style handling of the tentative definitions and there
  # are platforms lacking proper support for common symbols (osx).
  if(CC_HAS_FNO_COMMON)
    add_compile_flags("C;CXX" "-fno-common")
  endif()

  if(CC_HAS_GGDB)
    add_compile_flags("C;CXX" "-ggdb")
  endif()

  if(CC_HAS_WNO_UNKNOWN_PRAGMAS AND NOT HAVE_OPENMP)
    add_compile_flags("C;CXX" "-Wno-unknown-pragmas")
  endif()

  if(CC_HAS_SECTIONS)
    add_compile_flags("C;CXX" "-ffunction-sections" "-fdata-sections")
  elseif(MSVC)
    add_compile_flags("C;CXX" "/Gy")
  endif()

  # We must set -fno-omit-frame-pointer here, since we rely
  # on frame pointer when getting a backtrace, and it must
  # be used consistently across all object files.
  # The same reasoning applies to -fno-stack-protector switch.
  if(ENABLE_BACKTRACE)
    if(CC_HAS_FNO_OMIT_FRAME_POINTER)
      add_compile_flags("C;CXX" "-fno-omit-frame-pointer")
    endif()
  endif()

  if(MSVC)
    if(MSVC_VERSION LESS 1900)
      message(FATAL_ERROR "At least \"Microsoft C/C++ Compiler\" version 19.0.24234.1 (Visual Studio 2015 Update 3) is required.")
    endif()
    if(NOT MSVC_VERSION LESS 1910)
      add_compile_flags("CXX" "/Zc:__cplusplus")
    endif()
    add_compile_flags("C;CXX" "/utf-8")
  else()
    if(CC_HAS_WALL)
      add_compile_flags("C;CXX" "-Wall")
    endif()
    if(CC_HAS_WEXTRA)
      add_compile_flags("C;CXX" "-Wextra")
    endif()
  endif()

  add_definitions("-D__STDC_FORMAT_MACROS=1")
  add_definitions("-D__STDC_LIMIT_MACROS=1")
  add_definitions("-D__STDC_CONSTANT_MACROS=1")
  add_definitions("-D_HAS_EXCEPTIONS=1")

  # Only add -Werror if it's a debug build, done by developers.
  # Release builds should not cause extra trouble.
  if(CC_HAS_WERROR AND (CI OR CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE STREQUAL "Debug"))
    if(MSVC)
      add_compile_flags("C;CXX" "/WX")
    elseif(CMAKE_COMPILER_IS_CLANG)
      if(NOT CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 6)
        add_compile_flags("C;CXX" "-Werror")
      endif()
    elseif(CMAKE_COMPILER_IS_GNUCC)
      if(NOT CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 6)
        add_compile_flags("C;CXX" "-Werror")
      endif()
    else()
      add_compile_flags("C;CXX" "-Werror")
    endif()
  endif()


  if(CMAKE_COMPILER_IS_GNU${CMAKE_PRIMARY_LANG}
      AND CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 5)
    # G++ bug. http://gcc.gnu.org/bugzilla/show_bug.cgi?id=31488
    add_compile_flags("CXX" "-Wno-invalid-offsetof")
  endif()
  if(MINGW)
    # Disable junk MINGW's warnings that issued due to incompatibilities
    # and shortcomings of MINGW,
    # since the code is checked by builds with GCC, CLANG and MSVC.
    add_compile_flags("C;CXX" "-Wno-format-extra-args" "-Wno-format" "-Wno-cast-function-type" "-Wno-implicit-fallthrough")
  endif()

  if(ENABLE_ASAN)
    add_compile_flags("C;CXX" "-fsanitize=address")
  endif()

  if(ENABLE_UBSAN)
    add_compile_flags("C;CXX" "-fsanitize=undefined" "-fsanitize-undefined-trap-on-error")
  endif()

  if(ENABLE_GCOV)
    if(NOT HAVE_GCOV)
      message(FATAL_ERROR
        "ENABLE_GCOV option requested but gcov library is not found")
    endif()

    add_compile_flags("C;CXX" "-fprofile-arcs" "-ftest-coverage")
    set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    # add_library(gcov SHARED IMPORTED)
  endif()

  if(ENABLE_GPROF)
    add_compile_flags("C;CXX" "-pg")
  endif()

  if(CMAKE_COMPILER_IS_GNUCC AND LTO_ENABLED)
    add_compile_flags("C;CXX" ${GCC_LTO_CFLAGS})
    set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} ${GCC_LTO_CFLAGS} -fverbose-asm -fwhole-program")
    set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} ${GCC_LTO_CFLAGS} -fverbose-asm")
    set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} ${GCC_LTO_CFLAGS} -fverbose-asm")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
      # Pass the same optimization flags to the linker
      set(compile_flags "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}")
      set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} ${compile_flags}")
      set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} ${compile_flags}")
      set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} ${compile_flags}")
      unset(compile_flags)
    else()
      add_compile_flags("CXX" "-flto-odr-type-merging")
    endif()
  endif()

  if(MSVC AND NOT CMAKE_COMPILER_IS_CLANG AND LTO_ENABLED)
    add_compile_flags("C;CXX" "/GL")
    foreach(linkmode IN ITEMS EXE SHARED STATIC MODULE)
      set(${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS} /LTCG")
      string(REGEX REPLACE "^(.*)(/INCREMENTAL:NO *)(.*)$" "\\1\\3" ${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS}")
      string(REGEX REPLACE "^(.*)(/INCREMENTAL:YES *)(.*)$" "\\1\\3" ${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS}")
      string(REGEX REPLACE "^(.*)(/INCREMENTAL *)(.*)$" "\\1\\3" ${linkmode}_LINKER_FLAGS "${${linkmode}_LINKER_FLAGS}")
      string(STRIP "${${linkmode}_LINKER_FLAGS}" ${linkmode}_LINKER_FLAGS)
      foreach(config IN LISTS CMAKE_CONFIGURATION_TYPES ITEMS Release MinSizeRel RelWithDebInfo Debug)
        string(TOUPPER "${config}" config_uppercase)
        if(DEFINED "CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase}")
          string(REGEX REPLACE "^(.*)(/INCREMENTAL:NO *)(.*)$" "\\1\\3" altered_flags "${CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase}}")
          string(REGEX REPLACE "^(.*)(/INCREMENTAL:YES *)(.*)$" "\\1\\3" altered_flags "${altered_flags}")
          string(REGEX REPLACE "^(.*)(/INCREMENTAL *)(.*)$" "\\1\\3" altered_flags "${altered_flags}")
          string(STRIP "${altered_flags}" altered_flags)
          if(NOT "${altered_flags}" STREQUAL "${CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase}}")
            set(CMAKE_${linkmode}_LINKER_FLAGS_${config_uppercase} "${altered_flags}" CACHE STRING "Altered: '/INCREMENTAL' removed for LTO" FORCE)
          endif()
        endif()
      endforeach(config)
    endforeach(linkmode)
    unset(linkmode)

    foreach(config IN LISTS CMAKE_CONFIGURATION_TYPES ITEMS Release MinSizeRel RelWithDebInfo)
      foreach(lang IN ITEMS C CXX)
        string(TOUPPER "${config}" config_uppercase)
        if(DEFINED "CMAKE_${lang}_FLAGS_${config_uppercase}")
          string(REPLACE "/O2" "/Ox" altered_flags "${CMAKE_${lang}_FLAGS_${config_uppercase}}")
          if(NOT "${altered_flags}" STREQUAL "${CMAKE_${lang}_FLAGS_${config_uppercase}}")
            set(CMAKE_${lang}_FLAGS_${config_uppercase} "${altered_flags}" CACHE STRING "Altered: '/O2' replaced by '/Ox' for LTO" FORCE)
          endif()
        endif()
        unset(config_uppercase)
      endforeach(lang)
    endforeach(config)
    unset(altered_flags)
    unset(lang)
    unset(config)
  endif()

  if(CMAKE_COMPILER_IS_CLANG AND OSX_ARCHITECTURES)
    set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} -Wl,-keep_dwarf_unwind")
    set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} -Wl,-keep_dwarf_unwind")
    set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} -Wl,-keep_dwarf_unwind")
  endif()

  if(CMAKE_COMPILER_IS_CLANG AND LTO_ENABLED)
    if(CMAKE_${CMAKE_PRIMARY_LANG}_COMPILER_VERSION VERSION_LESS 3.9)
      set(CLANG_LTO_FLAG "-flto")
    else()
      set(CLANG_LTO_FLAG "-flto=thin")
    endif()
    add_compile_flags("C;CXX" ${CLANG_LTO_FLAG})
    if(NOT MSVC)
      set(EXE_LINKER_FLAGS "${EXE_LINKER_FLAGS} ${CLANG_LTO_FLAG} -fverbose-asm -fwhole-program")
      set(SHARED_LINKER_FLAGS "${SHARED_LINKER_FLAGS} ${CLANG_LTO_FLAG} -fverbose-asm")
      set(MODULE_LINKER_FLAGS "${MODULE_LINKER_FLAGS} ${CLANG_LTO_FLAG} -fverbose-asm")
    endif()
  endif()

  # push C/CXX flags into the cache
  if(CMAKE_CXX_COMPILER_LOADED)
    set(CMAKE_CXX_FLAGS ${CXX_FLAGS} CACHE STRING "Flags used by the C++ compiler during all build types" FORCE)
    unset(CXX_FLAGS)
  endif()
  if(CMAKE_C_COMPILER_LOADED)
    set(CMAKE_C_FLAGS ${C_FLAGS} CACHE STRING "Flags used by the C compiler during all build types" FORCE)
    unset(C_FLAGS)
  endif()
  set(CMAKE_EXE_LINKER_FLAGS ${EXE_LINKER_FLAGS} CACHE STRING "Flags used by the linker" FORCE)
  set(CMAKE_SHARED_LINKER_FLAGS ${SHARED_LINKER_FLAGS} CACHE STRING "Flags used by the linker during the creation of dll's" FORCE)
  set(CMAKE_STATIC_LINKER_FLAGS ${STATIC_LINKER_FLAGS} CACHE STRING "Flags used by the linker during the creation of static libraries" FORCE)
  set(CMAKE_MODULE_LINKER_FLAGS ${MODULE_LINKER_FLAGS} CACHE STRING "Flags used by the linker during the creation of modules" FORCE)
  unset(EXE_LINKER_FLAGS)
  unset(SHARED_LINKER_FLAGS)
  unset(STATIC_LINKER_FLAGS)
  unset(MODULE_LINKER_FLAGS)
endmacro(setup_compile_flags)

if(CMAKE_CXX_COMPILER_LOADED)
  # determine library for for std::filesystem
  set(LIBCXX_FILESYSTEM "")
  if(CMAKE_COMPILER_IS_CLANG)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
      set(LIBCXX_FILESYSTEM "c++experimental")
    elseif(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
      set(LIBCXX_FILESYSTEM "stdc++fs")
    endif()
  elseif(CMAKE_COMPILER_IS_GNUCXX AND NOT MINGW)
    if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.3 AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
      set(LIBCXX_FILESYSTEM "stdc++fs")
    endif()
  endif()
endif()

cmake_policy(POP)
