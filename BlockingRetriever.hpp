
//  Copyright 2019 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"
#include "MRedisTypes.hpp"

#include <boost/thread.hpp>
#include <boost/cstdint.hpp>

#include "tools/Assert.hpp"

#include <string>
#include <chrono>
#include <atomic>
#include <optional>

namespace moose {
namespace mredis {

/*! @brief behavioral helper to allow for easier use of a the result future to wait for the result of an async op
	blocking the current thread
	@see FiberRetriever
 */
template <typename Retval>
class BlockingRetriever {

	public:
		BlockingRetriever() = delete;
		BlockingRetriever(const BlockingRetriever &) = delete;

		/*! @brief initialize a new getter for one-time usage
			@param n_timeout wait for this long before exception is returned 
		 */
		BlockingRetriever(const unsigned int n_timeout = 3)
				: m_timeout{ n_timeout }
				, m_promise{ std::make_shared < boost::promise< std::optional<Retval> > >() }
				, m_used{ false } {
		}

		/*! @brief call either that or get the future
			@return value if set, otherwise none
			@throw redis_error on timeout or underlying condition
		 */
		std::optional<Retval> wait_for_response() {
			
			MOOSE_ASSERT_MSG((!m_used), "Double use of BlockingRetriever object");

			using namespace moose::tools;

			boost::unique_future< std::optional<Retval> > future_value = m_promise->get_future();

			// Now I think this wait_for would imply a yield... Meaning that other fiber will take over while this one waits
			if (future_value.wait_for(boost::chrono::seconds(m_timeout)) == boost::future_status::timeout) {
				m_used.store(true);
				BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout getting redis value"));
			}

			m_used.store(true);

			// Now we must have a value of correct type as our callback already checked for that.
			// This may still throw however, but redis_error is expected by caller
			return boost::get< std::optional<Retval> >(future_value.get());
		}

		//! @brief use this as a callback in AsyncClient calls
		//! it returns a no-throw handler which will satisfy the later wait_for_response()
#if BOOST_MSVC
		Callback responder() const {
			
			static_assert(sizeof(Retval) == -1, "Do not use general responder function. Specialize for Retval type")
		};
#else
		Callback responder() const = delete;
#endif

	private:
		const unsigned int     m_timeout;
		std::shared_ptr< boost::promise< std::optional<Retval> > > m_promise;
		std::atomic<bool>      m_used;  //!< to prevent double usage of this object, set to true after use
};

template<>
inline Callback BlockingRetriever<std::string>::responder() const {

	return [promise{ m_promise }](const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// I had strange errors trying to std::make_exception_ptr the error in there.
			// Very likely due to slicing. I will transport an error code as it is
			if (is_error(n_message)) {
				redis_error rerr = boost::get<redis_error>(n_message);
				throw rerr;
			}

			if (is_null(n_message)) {
				promise->set_value(std::nullopt);
				return;
			}

			if (!is_string(n_message)) {			
				BOOST_THROW_EXCEPTION(redis_error()
						<< error_message("Unexpected return type, not a string")
						<< error_argument(n_message.which()));
			}

			promise->set_value(boost::get<std::string>(n_message));

		} catch (const redis_error &err) {
			promise->set_exception(err);
		}
	};
}

template<>
inline Callback BlockingRetriever<boost::int64_t>::responder() const {

	return [promise{ m_promise }](const RedisMessage &n_message) {

		using namespace moose::tools;
		try {
			// I had strange errors trying to std::make_exception_ptr the error in there.
			// Very likely due to slicing. I will transport an error code as it is
			if (is_error(n_message)) {
				redis_error rerr = boost::get<redis_error>(n_message);
				throw rerr;
			}

			if (is_null(n_message)) {
				promise->set_value(std::nullopt);
				return;
			}

			if (!is_int(n_message)) {
				BOOST_THROW_EXCEPTION(redis_error() 
						<< error_message("Unexpected return type, not an int")
						<< error_argument(n_message.which()));
			}

			promise->set_value(boost::get<boost::int64_t>(n_message));

		} catch (const redis_error &err) {
			promise->set_exception(err);
		}
	};
}

template<>
inline Callback BlockingRetriever< std::vector<RedisMessage> >::responder() const {

	return [promise{ m_promise }](const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// translate the error into an exception that will throw when the caller get()s the future
			if (is_error(n_message)) {
				const redis_error rerr = boost::get<redis_error>(n_message);
				throw rerr;
			}

			if (is_null(n_message)) {
				promise->set_value(std::nullopt);
				return;
			}

			if (!is_array(n_message)) {
				BOOST_THROW_EXCEPTION(redis_error()
						<< error_message("Unexpected return type, not an array")
						<< error_argument(n_message.which()));
			}

			promise->set_value(boost::get< std::vector<RedisMessage> >(n_message));

		} catch (const redis_error &err) {
			promise->set_exception(err);
		}
	};
}

#if defined(BOOST_MSVC)
MREDIS_API void BlockingRetrievergetRidOfLNK4221();
#endif

}
}
