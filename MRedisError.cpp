
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisError.hpp"

namespace moose {
namespace mredis {

char const *redis_error::what() const noexcept {

	return "redis error";
}

void redis_error::set_server_message(const std::string &n_message) {

	*this << redis_server_message(n_message);
}

}
}
