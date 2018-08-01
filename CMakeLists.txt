
# Copyright 2018 Stephan Menzel. Distributed under the Boost
# Software License, Version 1.0. (See accompanying file
# LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.4)
project(mredis)

add_definitions(-DBOOST_THREAD_VERSION=3)

find_package(Boost 1.67.0 COMPONENTS chrono thread system program_options log unit_test_framework  REQUIRED)

set(MREDIS_SRC
	FwdDeclarations.cpp
	AsyncClient.cpp
	RESP.cpp
	MRedisResult.cpp
	MRedisError.cpp
	MRedisTCPConnection.cpp
	)

set(MREDIS_HDR
	FwdDeclarations.hpp
	MRedisConfig.hpp
	AsyncClient.hpp
	RESP.hpp
	MRedisResult.hpp
	MRedisError.hpp
	MRedisTCPConnection.hpp
	)

include_directories(
	SYSTEM
	${Boost_INCLUDE_DIRS}
)

include_directories(
	${mredis_SOURCE_DIR}/..
	${CMAKE_CURRENT_SOURCE_DIR}
)

if (${MOOSE_STATIC_BUILD})
	add_library(mredis STATIC ${MREDIS_SRC} ${MREDIS_HDR})
else()
	add_definitions(-DMREDIS_DLL)
	add_library(mredis SHARED ${MREDIS_SRC} ${MREDIS_HDR})
endif()

target_link_libraries(mredis
	${Boost_LIBRARIES}
)

add_subdirectory(test)
enable_testing()
add_test(NAME TestRespParsers COMMAND TestRespParsers)

add_executable(redistest redistest.cpp)
target_link_libraries(redistest
	mredis
	moose_tools
	${Boost_LIBRARIES}
)


