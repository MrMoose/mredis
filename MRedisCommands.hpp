
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"
#include "MRedisTypes.hpp"

#include <iostream>
#include <string>

namespace moose {
namespace mredis {

/*! @defgroup generators to write commands onto the stream
	Everything that takes stream ptrs asserts when they are null
	@{
*/

//! write a ping into the stream
MREDIS_API void format_ping(std::ostream &n_os);

//! @return bulk string or nil
MREDIS_API void format_get(std::ostream &n_os, const std::string &n_key);

//! @return integer
MREDIS_API void format_set(std::ostream &n_os, const std::string &n_key, const std::string &n_value,
			const Duration &n_expire_time, const SetCondition n_condition);

//! @return integer
MREDIS_API void format_expire(std::ostream &n_os, const std::string &n_key, const Duration &n_expire_time);

//! @return int (1)
MREDIS_API void format_del(std::ostream &n_os, const std::string &n_key);

//! @return int (0 or 1)
MREDIS_API void format_exists(std::ostream &n_os, const std::string &n_key);

//! @return integer
MREDIS_API void format_incr(std::ostream &n_os, const std::string &n_key);

//! @return integer
MREDIS_API void format_decr(std::ostream &n_os, const std::string &n_key);

//! @return integer
MREDIS_API void format_hincrby(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_incr_by);

//! @return string or nil
MREDIS_API void format_hget(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name);

//! @return integer
MREDIS_API void format_hset(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value);

//! @return integer
MREDIS_API void format_hdel(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name);

//! @return array
MREDIS_API void format_hgetall(std::ostream &n_os, const std::string &n_hash_name);

//! @return integer
MREDIS_API void format_lpush(std::ostream &n_os, const std::string &n_list_name, const std::string &n_value);

//! @return integer
MREDIS_API void format_rpush(std::ostream &n_os, const std::string &n_list_name, const std::string &n_value);

//! @return integer
MREDIS_API void format_sadd(std::ostream &n_os, const std::string &n_set_name, const std::string &n_value);

//! @return integer
MREDIS_API void format_srem(std::ostream &n_os, const std::string &n_set_name, const std::string &n_value);

//! @return array
MREDIS_API void format_smembers(std::ostream &n_os, const std::string &n_set_name);

//! @return whatever the script returns
MREDIS_API void format_eval(std::ostream &n_os, const std::string &n_script, const std::vector<std::string> &n_keys, const std::vector<std::string> &n_args);

//! will always subscribe to MREDIS_WAKEUP as well to get a dummy message in order to interrupt dormant pubsub connections
MREDIS_API void format_subscribe(std::ostream &n_os, const std::string &n_channel_name);
MREDIS_API void format_unsubscribe(std::ostream &n_os, const std::string &n_channel_name);
MREDIS_API void format_publish(std::ostream &n_os, const std::string &n_channel_name, const std::string &n_message);

/*! @} */

}
}

