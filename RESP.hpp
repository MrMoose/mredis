
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"

#include <boost/variant.hpp>
#include <boost/cstdint.hpp>
#include <boost/asio/streambuf.hpp>

#include <iostream>
#include <string>

namespace moose {
namespace mredis {

// debug
MREDIS_API bool parse(const std::string &n_input, RESPonse &n_response);

/*! @defgroup generators to write commands onto the stream
	Everything that takes stream ptrs asserts when they are null
	@{
*/

//! write a ping into the stream
MREDIS_API void format_ping(std::ostream &n_os);


//! @return integer
MREDIS_API void format_hincrby(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_incr_by);


//! @return integer
MREDIS_API void format_sadd(std::ostream &n_os, const std::string &n_set_name, const std::string &n_value);

/*! @} */


MREDIS_API RESPonse parse_one(std::istream &n_is);

/*! Parse one message from the stream.
	@return false on cannot parse any
 */
MREDIS_API bool parse_from_stream(std::istream &n_is, RESPonse &n_response) noexcept;




}
}

