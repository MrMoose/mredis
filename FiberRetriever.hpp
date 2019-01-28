
//  Copyright 2019 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"
#include "MRedisTypes.hpp"

#include <boost/fiber/all.hpp>
#include <boost/optional.hpp>
#include <boost/cstdint.hpp>

#include "tools/Assert.hpp"

#include <string>
#include <chrono>
#include <atomic>

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
				: m_timeout(n_timeout)
				, m_promise(new boost::fibers::promise< boost::optional<Retval> >)
				, m_used(false) {
		}

		/*! @brief call either that or get the future
			@return value if set, otherwise none
			@throw redis_error
		 */
		boost::optional<Retval> wait_for_response() {
			
			MOOSE_ASSERT_MSG((!m_used), "Double use of FiberRetriever object");

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
			return boost::get< boost::optional<Retval> >(future_value.get());
		}

		//! @brief use this as a callback in AsyncClient calls
#if BOOST_MSVC
		Callback responder() const {

			static_assert(sizeof(Retval) == -1, "Do not use general reponder function. Specialize for Retval type")
		};
#else
		Callback responder() const = delete;
#endif

	private:
		const unsigned int     m_timeout;
		std::unique_ptr< boost::fibers::promise< boost::optional<Retval> > > m_promise;
		std::atomic<bool>      m_used;  //!< to prevent double usage of this object, set to true after use
};

template<>
inline Callback FiberRetriever<std::string>::responder() const {

	return [this](const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// translate the error into an exception that will throw when the caller get()s the future
			if (is_error(n_message)) {
				redis_error rerr = boost::get<redis_error>(n_message);
				throw rerr;
				return;
			}

			if (is_null(n_message)) {
				this->m_promise->set_value(boost::none);
				return;
			}

			if (!is_string(n_message)) {
				BOOST_THROW_EXCEPTION(redis_error()
					<< error_message("Unexpected return type, not a string")
					<< error_argument(n_message.which()));
			}

			this->m_promise->set_value(boost::get<std::string>(n_message));

		} catch (const redis_error &err) {

			this->m_promise->set_exception(std::make_exception_ptr(err));
		}
	};
}

template<>
inline Callback FiberRetriever<boost::int64_t>::responder() const {

	return [this](const RedisMessage &n_message) {

		using namespace moose::tools;

		try {
			// translate the error into an exception that will throw when the caller get()s the future
			if (is_error(n_message)) {
				redis_error rerr = boost::get<redis_error>(n_message);
				throw rerr;
				return;
			}

			if (is_null(n_message)) {
				this->m_promise->set_value(boost::none);
				return;
			}

			if (!is_int(n_message)) {
				BOOST_THROW_EXCEPTION(redis_error()
					<< error_message("Unexpected return type, not an int")
					<< error_argument(n_message.which()));
			}

			this->m_promise->set_value(boost::get<boost::int64_t>(n_message));

		} catch (const redis_error &err) {

			this->m_promise->set_exception(std::make_exception_ptr(err));
		}
	};
}

#if defined(BOOST_MSVC)
MREDIS_API void FiberRetrievergetRidOfLNK4221();
#endif

}
}
