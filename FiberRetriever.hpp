
//  Copyright 2019 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"
#include "MRedisTypes.hpp"

#include <boost/fiber/all.hpp>

#include "tools/Assert.hpp"

#include <string>
#include <chrono>
#include <atomic>
#include <optional>
#include <cstdint>

namespace moose {
namespace mredis {

/*! @brief behavioral helper to allow for easier use of a fiber future to wait for the result of an async op

 */
template <typename Retval>
class FiberRetriever {

	public:
		FiberRetriever() = delete;
		FiberRetriever(const FiberRetriever &) = delete;

		/*! @brief initialize a new getter for one-time usage
			@param n_timeout wait for this long before exception is returned 
		 */
		FiberRetriever(const unsigned int n_timeout = 3)
				: m_timeout{ n_timeout }
				, m_promise{ std::make_shared<boost::fibers::promise< std::optional<Retval> > >() }
				, m_used{ false } {
		}

		/*! @brief call either that or get the future
			@return value if set, otherwise none
			@throw redis_error on timeout or underlying condition
		 */
		std::optional<Retval> wait_for_response() {
			
			MOOSE_ASSERT_MSG((!m_used), "Double use of FiberRetriever object");

			using namespace moose::tools;

			boost::fibers::future< std::optional<Retval> > future_value = m_promise->get_future();

			// Now I think this wait_for would imply a yield... Meaning that other fiber will take over while this one waits
			if (future_value.wait_for(std::chrono::seconds(m_timeout)) == boost::fibers::future_status::timeout) {
				m_used.store(true);
				BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout getting redis value"));
			}

			m_used.store(true);

			// Now we must have a value of correct type as our callback already checked for that.
			// This may still throw however
			return boost::get< std::optional<Retval> >(future_value.get());
		}

		/*! @brief use this as a callback in AsyncClient calls
			#moep also this is unsafe. When handing this into the get call, it might throw, calling the d'tor 
				on the retriever object but leaving this handler intact. When it is later than called, it will
				crash. Fix by making this shared_from_this and handing in the shared_ptr rather than the 
				callback only.
		 */
#if BOOST_MSVC
		Callback responder() const {

			static_assert(sizeof(Retval) == -1, "Do not use general responder function. Specialize for Retval type")
		};
#else
		Callback responder() const = delete;
#endif

	private:
		const unsigned int     m_timeout;
		std::shared_ptr< boost::fibers::promise< std::optional<Retval> > > m_promise;
		std::atomic<bool>      m_used;  //!< to prevent double usage of this object, set to true after use
};

template<>
inline Callback FiberRetriever<std::string>::responder() const {

	return [promise{ m_promise }](const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// translate the error into an exception that will throw when the caller get()s the future
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
			promise->set_exception(std::make_exception_ptr(err));
		}
	};
}

template<>
inline Callback FiberRetriever<boost::int64_t>::responder() const {

	return [promise{ m_promise }] (const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// translate the error into an exception that will throw when the caller get()s the future
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

			promise->set_value(boost::get<std::int64_t>(n_message));

		} catch (const redis_error &err) {
			promise->set_exception(std::make_exception_ptr(err));
		}
	};
}

template<>
inline Callback FiberRetriever< std::vector<RedisMessage> >::responder() const {

	return [promise{ m_promise }](const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// translate the error into an exception that will throw when the caller get()s the future
			if (is_error(n_message)) {
				redis_error rerr = boost::get<redis_error>(n_message);
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
			promise->set_exception(std::make_exception_ptr(err));
		}
	};
}

#if defined(BOOST_MSVC)
MREDIS_API void FiberRetrievergetRidOfLNK4221();
#endif

}
}
