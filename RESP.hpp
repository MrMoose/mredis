
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"
#include "MRedisTypes.hpp"

#include <boost/variant.hpp>
#include <boost/cstdint.hpp>

#include <iostream>
#include <string>
#include <chrono>

namespace moose {
namespace mredis {

/*! Raw protocol implementation
 */

// debug
MREDIS_API bool parse(const std::string &n_input, RedisMessage &n_response);

MREDIS_API RedisMessage parse_one(std::istream &n_is);

/*! Parse one message from the stream.
	@return false on cannot parse any
 */
MREDIS_API bool parse_from_stream(std::istream &n_is, RedisMessage &n_response) noexcept;

/*! Generate to stream
	Mostly for testing
 */
MREDIS_API void generate_to_stream(std::ostream &n_os, const RedisMessage &n_message);

}
}

