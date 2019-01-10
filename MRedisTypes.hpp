
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"

#include "tools/Error.hpp"

#include <chrono>

namespace moose {
namespace mredis {

using Clock = std::chrono::steady_clock;
using Duration = std::chrono::steady_clock::duration;
using TimePoint = std::chrono::time_point<Clock>;

//! Sadly, like boost chrono, the std variant also does not have an empty uninitialized default.
constexpr Duration c_invalid_duration = std::chrono::steady_clock::duration::max();

//! I didn't even bother looking if other commands have the same additional parameters
//! Right now, this is only used for SET
MOOSE_TOOLS_API enum class SetCondition {
	NONE = 0,    //!< always set
	NX,          //!< set only if it doesn't exist already
	XX           //!< set only if it does already exist
};

#if defined(BOOST_MSVC)
void MredisTypesGetRidOfLNK4221();
#endif

}
}
