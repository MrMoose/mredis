
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisTypes.hpp"

namespace moose {
namespace mredis {

LuaArgument::LuaArgument(std::string &&n_key, std::string &&n_value)
		: m_key(n_key)
		, m_value(n_value) {

}

}
}
