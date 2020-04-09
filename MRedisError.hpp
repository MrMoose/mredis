
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"

#include "tools/Error.hpp"

#include <string>

namespace moose {
namespace mredis {

//! Some component violated protocol specifications and talked BS
struct MREDIS_API redis_error : virtual public tools::moose_error {

	char const *what() const noexcept override;

	void set_server_message(const std::string &n_message);

	std::string server_message() const noexcept;
};

//! tag any exception with a human readable error message coming from redis server
using redis_server_message = boost::error_info<struct tag_redis_server_message, std::string>;

}
}
