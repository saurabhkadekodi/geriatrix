#
# CMakeLists.txt  top-level cmake file for geriatrix filesystem aging app
# 28-Oct-2016  chuck@ece.cmu.edu
#

cmake_minimum_required (VERSION 2.8)
project (geriatrix C CXX)

# we'll need to check for posix_fallocate
include (CheckFunctionExists)

# geriatrix requires c++11
set (CMAKE_CXX_STANDARD 11)
set (CXX_STANDARD_REQUIRED True)

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS 
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (DEBUG_SANITIZER Off CACHE STRING "Sanitizer for debug builds")
set_property (CACHE DEBUG_SANITIZER PROPERTY STRINGS
              "Off" "Address" "Thread")
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")

#
# sanitizer config (XXX: does not probe compiler to see if sanitizer flags
# are supported... )
#
set (as_flags "-fsanitize=address -O1 -fno-omit-frame-pointer")
set (ts_flags "-fsanitize=thread  -O1 -fno-omit-frame-pointer")
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    if (${DEBUG_SANITIZER} STREQUAL "Address")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
    elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
    endif ()
endif ()

# geriatrix requires threads
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)
list (APPEND geriatrix-depends "${CMAKE_THREAD_LIBS_INIT}")

# geriatrix requires boost
set (Boost_NO_BOOST_CMAKE ON)
find_package(Boost)
if (NOT Boost_FOUND AND NOT BOOST_FOUND)
    message ("geriatrix requires boost...  install boost and set")
    message ("CMAKE_PREFIX_PATH to point to boost")
    message (FATAL_ERROR "Aborting...")
endif ()
include_directories (${Boost_INCLUDE_DIRS})

# check for posix_fallocate
check_function_exists (posix_fallocate HAS_POSIX_FALLOCATE)
if (NOT HAS_POSIX_FALLOCATE)
    add_definitions (-DNEED_POSIX_FALLOCATE)
endif ()

# deltafs is an option
if (DELTAFS)
    find_package (deltafs CONFIG REQUIRED)
    add_definitions (-DDELTAFS)
    list (APPEND geriatrix-depends deltafs)
    list (APPEND geriatrix-drivers src/deltafs_driver.c)
endif ()

add_executable (geriatrix ${geriatrix-drivers} src/geriatrix.cpp)
target_link_libraries (geriatrix ${geriatrix-depends})

install (TARGETS geriatrix RUNTIME DESTINATION bin)
