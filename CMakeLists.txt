cmake_minimum_required(VERSION 3.27)
project(SGUFP_Solver)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

set(SOURCES master.cpp server.cpp client.cpp)

add_executable(master master.cpp)
add_executable(server server.cpp)
add_executable(client client.cpp)


