
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisError.hpp"

#include <boost/variant.hpp>
#include <boost/cstdint.hpp>
#include <boost/thread/future.hpp>

#include "tools/Assert.hpp"

#include <functional>
#include <vector>
#include <chrono>
#include <atomic>

namespace moose {
namespace mredis {

struct null_result {};

typedef boost::make_recursive_variant<
	redis_error,                           // 0 wrapped up exception for error cases
	std::string,                           // 1 string response  (simple or bulk)
	boost::int64_t,                        // 2 integer response
	null_result,                           // 3 null response
	std::vector<boost::recursive_variant_> // 4 arrays
>::type RedisMessage;

//! convenience type check so you don't have to fuck around with magic numbers like which() does
inline bool is_error(const RedisMessage &n_message) noexcept {

	return n_message.which() == 0;
}

//! convenience type check so you don't have to fuck around with magic numbers like which() does
inline bool is_string(const RedisMessage &n_message) noexcept {

	return n_message.which() == 1;
}

inline bool is_int(const RedisMessage &n_message) noexcept {

	return n_message.which() == 2;
}

inline bool is_null(const RedisMessage &n_message) noexcept {

	return n_message.which() == 3;
}

inline bool is_array(const RedisMessage &n_message) noexcept {

	return n_message.which() == 4;
}

//! callback for all kinds of responses
using Callback        = std::function<void(const RedisMessage &)>;

//! the way I understand the protocol, payload to a message published is always a string
using MessageCallback = std::function<void(const std::string &)>;

struct mrequest {

	std::function<void(std::ostream &n_os)> m_prepare;
	Callback                                m_callback;
};

using future_response       = boost::unique_future<RedisMessage>;

using promised_response     = boost::promise<RedisMessage>;
using promised_response_ptr = std::shared_ptr<promised_response>;

#if defined(BOOST_MSVC)
MREDIS_API void MRedisResultgetRidOfLNK4221();
#endif

}
}
