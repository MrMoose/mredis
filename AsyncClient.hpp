
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"

#include <boost/thread/mutex.hpp>
#include <boost/cstdint.hpp>
#include <boost/asio/io_context.hpp>

namespace moose {
namespace mredis {

class MRedisTCPConnection;

/*! @brief simple async Redis client.
	And yes, I know. Classes that are described as "simple" generally turn out 
	not at all simple or broken or both. But I have no choice. None of the available 
	redis client libs are suitable for this.

	@note This is meant to use an io_context that is already run elsewhere.
		It relies on the fact that this actually is run. Also, it does not support 
		multiple threads running that io_context. Implicit strand.
 */
class AsyncClient {

	public:
		
		//! use local unix domain socket
		MREDIS_API AsyncClient(boost::asio::io_context &n_io_context);

		/*! use IP to do TCP connect
			@note asserts on empty server
		 */
		MREDIS_API AsyncClient(boost::asio::io_context &n_io_context, const std::string &n_server, const boost::uint16_t n_port = 6379);

		MREDIS_API virtual ~AsyncClient() noexcept;

		/*! connect right away
			@note if already connected, will re-connect
			@throw network_error on cannot resolve host name
		 */
		MREDIS_API void connect();

		void set(const std::string &n_name, const std::string   &n_value);
		void set(const std::string &n_name, const boost::int64_t n_value);


		/*! @defgroup hash map functions
			They all assert when connect wasn't called.
			@{
		*/

		/*! @brief hash map field increment
			@param n_hash_name assert on empty
			@param n_field_name assumed to be an integer field
			@param n_callback if set, must be no-throw
			@see https://redis.io/commands/hincrby
		 */
		MREDIS_API void hincrby(const std::string &n_hash_name,
		                        const std::string &n_field_name,
								const boost::int64_t n_increment_by = 1,
		                        Callback &&n_callback = Callback()) noexcept;

		/*! @} */

	private:

		friend class MRedisTCPConnection;




		boost::asio::io_context             &m_io_context;
		const std::string                    m_server;         //!< if tcp, is set to server
		std::unique_ptr<MRedisTCPConnection> m_connection;

		// Each client
};

}
}

