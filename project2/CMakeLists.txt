cmake_minimum_required(VERSION 3.14)
project(project2)

# GoogleTest requires at least C++11
set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/e4717df71a4f45bf9f0ac88c6cd9846a0bc248dd.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(
    ./include
)

add_library(
    disk_space_manager
    ./src/file.cpp
)

add_executable(
    basic_test
    ./googletest/basic_test.cc
)

target_link_libraries(
    basic_test
    gtest_main
    disk_space_manager
)

include(GoogleTest)
gtest_discover_tests(basic_test)