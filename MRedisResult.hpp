
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisError.hpp"

#include <boost/variant.hpp>
#include <boost/cstdint.hpp>
#include <boost/thread/future.hpp>
#include <boost/fiber/all.hpp>
#include <boost/optional.hpp>

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





template <typename Retval>
class get_result_in_fiber {

	public:
		get_result_in_fiber() = delete;
		get_result_in_fiber(const get_result_in_fiber &) = delete;

		/*! @brief initialize a new getter for one-time usage
			@param n_timeout wait for this long before exception is returned 
		 */
		get_result_in_fiber(const unsigned int n_timeout = 3)
				: m_timeout(n_timeout)
				, m_promise(new boost::fibers::promise< boost::optional<Retval> >)
				, m_used(false) {
		}

		/*! @brief call either that or get the future
			@return value if set, otherwise none
			@throw redis_error
		 */
		boost::optional<Retval> wait_for_response() {
			
			MOOSE_ASSERT_MSG((!m_used), "Double use of get_result_in_fiber object");

			using namespace moose::tools;

			boost::fibers::future< boost::optional<Retval> > future_value = m_promise->get_future();

			// Now I think this wait_for would imply a yield... Meaning that other fiber will take over while this one waits
			if (future_value.wait_for(std::chrono::seconds(m_timeout)) == boost::fibers::future_status::timeout) {
				m_used.store(true);
				BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout getting redis value"));
			}

			m_used.store(true);

			// Now we must have a value of correct type as our callback already checked for that.
			// This may still throw however
			return boost::get<Retval>(future_value.get());
		}

		//! @brief use this as a callback in AsyncClient calls
		Callback responder() const {

			static_assert(false, "Non MRedis answer type cannot be used with this template");
		}

	private:
		const unsigned int     m_timeout;
		std::unique_ptr< boost::fibers::promise< boost::optional<Retval> > > m_promise;
		std::atomic<bool>      m_used;  //!< to prevent double usage of this object, set to true after use
};

template<>
inline Callback get_result_in_fiber<std::string>::responder() const {

	return [this](const RedisMessage &n_message) {

		using namespace moose::tools;

		// translate the error into an exception that will throw when the caller get()s the future
		if (is_error(n_message)) {
			redis_error ex = boost::get<redis_error>(n_message);
			this->m_promise->set_exception(std::make_exception_ptr(ex));
			return;
		}

		if (is_null(n_message)) {
			this->m_promise->set_value(boost::none);
			return;
		}

		if (!is_string(n_message)) {
			redis_error ex;
			ex << error_message("Unexpected return type, not a string");
			ex << error_argument(n_message.which());
			this->m_promise->set_exception(std::make_exception_ptr(ex));
			return;
		}

		this->m_promise->set_value(boost::get<std::string>(n_message));
	};
}

template<>
inline Callback get_result_in_fiber<boost::int64_t>::responder() const {

	return [this](const RedisMessage &n_message) {

		using namespace moose::tools;

		// translate the error into an exception that will throw when the caller get()s the future
		if (is_error(n_message)) {
			redis_error ex = boost::get<redis_error>(n_message);
			this->m_promise->set_exception(std::make_exception_ptr(ex));
			return;
		}

		if (is_null(n_message)) {
			this->m_promise->set_value(boost::none);
		}

		if (!is_int(n_message)) {
			redis_error ex;
			ex << error_message("Unexpected return type, not an int");
			ex << error_argument(n_message.which());
			this->m_promise->set_exception(std::make_exception_ptr(ex));
			return;
		}

		this->m_promise->set_value(boost::get<boost::int64_t>(n_message));
	};
}


#if defined(BOOST_MSVC)
MREDIS_API void MRedisResultgetRidOfLNK4221();
#endif

}
}
