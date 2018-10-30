
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"

#include "tools/Error.hpp"

#include <chrono>

namespace moose {
namespace mredis {


typedef std::chrono::steady_clock Clock;
typedef std::chrono::steady_clock::duration Duration;
typedef std::chrono::time_point<Clock> TimePoint;

//! Sadly, like boost chrono, the std variant also does not have an empty uninitialized default.
constexpr Duration c_invalid_duration = Duration::max();

//! I didn't even bother looking if other commands have the same additional parameters
//! Right now, this is only used for SET
MOOSE_TOOLS_API enum class SetCondition {
	NONE = 0,    //!< always set
	NX,          //!< set only if it doesn't exist already
	XX           //!< set only if it does already exist
};

class LuaArgument {
	
	public:
		//! Will throw when either key or value are empty
		MREDIS_API LuaArgument(std::string &&n_key, std::string &&n_value);

		// inline getters should only be available within the lib
		const std::string &key() const {
			return m_key;
		}

		const std::string &value() const {
			return m_value;
		}

	private:
		const std::string m_key;
		const std::string m_value;
};

}
}
