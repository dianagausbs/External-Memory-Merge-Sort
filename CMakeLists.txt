cmake_minimum_required(VERSION 3.0.0)
project(exercise01)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB_RECURSE PROJECT_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/src/*.cpp")

add_executable(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE ${PROJECT_SOURCES})

target_compile_definitions(${PROJECT_NAME} PUBLIC DATA_PATH="${CMAKE_CURRENT_SOURCE_DIR}/data/")