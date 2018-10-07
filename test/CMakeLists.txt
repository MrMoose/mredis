
# Copyright 2018 Stephan Menzel. Distributed under the Boost
# Software License, Version 1.0. (See accompanying file
# LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

add_executable(TestRespParsers TestRespParsers.cpp)
target_link_libraries(TestRespParsers mredis Boost::unit_test_framework)
