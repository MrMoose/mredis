
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisError.hpp"

#include <boost/variant.hpp>
#include <boost/cstdint.hpp>

namespace moose {
namespace mredis {

typedef boost::variant<
	redis_error,              // 0 wrapped up exception for error cases
	std::string,              // 1 string response  (simple or bulk)
	boost::int64_t            // 2 integer response
> RESPonse;

}
}

#if defined(BOOST_MSVC)
MREDIS_API void MRedisResultgetRidOfLNK4221();
#endif
