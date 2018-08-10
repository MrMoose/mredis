
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisError.hpp"

//#define BOOST_THREAD_PROVIDES_FUTURE
#include <boost/variant.hpp>
#include <boost/cstdint.hpp>
#include <boost/thread/future.hpp>

#include <functional>
#include <vector>

namespace moose {
namespace mredis {

struct null_result {};

typedef boost::make_recursive_variant<
	redis_error,                           // 0 wrapped up exception for error cases
	std::string,                           // 1 string response  (simple or bulk)
	boost::int64_t,                        // 2 integer response
	null_result,                           // 3 null response
	std::vector<boost::recursive_variant_> // 4 arrays
>::type RESPonse;

typedef std::function<void(const RESPonse &)> Callback;

struct mrequest {

	std::function<void(std::ostream &n_os)> m_prepare;
	Callback                                m_callback;
};

typedef boost::future<RESPonse>              future_response;

typedef boost::promise<RESPonse>             promised_response;
typedef boost::shared_ptr<promised_response> promised_response_ptr;

}
}

#if defined(BOOST_MSVC)
MREDIS_API void MRedisResultgetRidOfLNK4221();
#endif
