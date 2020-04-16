
# Copyright 2018 Stephan Menzel. Distributed under the Boost
# Software License, Version 1.0. (See accompanying file
# LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.12)
project(mredis)

find_package(Boost 1.67.0 REQUIRED COMPONENTS thread system fiber program_options log unit_test_framework)

set(MREDIS_SRC
	FwdDeclarations.cpp
	AsyncClient.cpp
	RESP.cpp
	FiberRetriever.cpp
	BlockingRetriever.cpp
	MRedisResult.cpp
	MRedisError.cpp
	MRedisTypes.cpp
	MRedisConnection.cpp
	MRedisCommands.cpp
	MRedisPubsubConnection.cpp
	)

set(MREDIS_HDR
	FwdDeclarations.hpp
	MRedisConfig.hpp
	AsyncClient.hpp
	RESP.hpp
	FiberRetriever.hpp
	BlockingRetriever.hpp
	MRedisResult.hpp
	MRedisError.hpp
	MRedisTypes.hpp
	MRedisConnection.hpp
	MRedisCommands.hpp
	MRedisPubsubConnection.hpp
	)

add_library(mredis ${MREDIS_SRC} ${MREDIS_HDR})
if (${BUILD_SHARED_LIBS})
	target_compile_definitions(mredis PUBLIC -DMREDIS_DLL)
endif()

target_link_libraries(mredis 
	PUBLIC
		moose_tools
		Boost::boost
	PRIVATE
		Boost::thread
		Boost::system
)

target_include_directories(mredis
   PUBLIC 
        $<INSTALL_INTERFACE:mredis>    
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if (${CMAKE_VERSION} VERSION_LESS "3.16.0") 
	message("Please consider to switch to CMake 3.16.0 or above for PCH support")
else()
	target_precompile_headers(mredis
		PUBLIC
		"<string>"
		"<vector>"
		"<map>"
		"<iostream>"
		"<fstream>"
		"<algorithm>"
		"<chrono>"
		"<functional>"
		"<atomic>"
		"<optional>"
		"<boost/asio.hpp>"
		"<boost/thread.hpp>"
		"<boost/spirit/include/qi.hpp>"
		"<boost/spirit/include/karma.hpp>"
		"<boost/spirit/include/phoenix.hpp>"
	)
	set_target_properties(mredis PROPERTIES UNITY_BUILD ON)
endif()

set_property(TARGET moose_tools PROPERTY CXX_STANDARD 17)

add_subdirectory(test)
enable_testing()
add_test(NAME TestRespParsers COMMAND TestRespParsers)

add_executable(redistest redistest.cpp)
target_link_libraries(redistest
	mredis
	Boost::fiber
	Boost::program_options
)

add_executable(mt_testsuite mt_testsuite.cpp)
target_link_libraries(mt_testsuite
	mredis
	Boost::program_options
)

add_executable(fiber_testsuite fiber_testsuite.cpp)
target_link_libraries(fiber_testsuite
	mredis
	Boost::fiber
	Boost::program_options
)

