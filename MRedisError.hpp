
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"

#include "tools/Error.hpp"

namespace moose {
namespace mredis {

//! Some component violated protocol specifications and talked BS
struct MOOSE_TOOLS_API redis_error : virtual tools::moose_error {

	virtual char const *what() const noexcept;
};


}
}
