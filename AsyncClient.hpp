
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
 */
class AsyncClient {

	public:
		
		typedef std::function<void (const RESPonse &n_result)> Callback;

		//! use local unix domain socket
		AsyncClient(boost::asio::io_context &n_io_context);

		/*! use IP
			@note asserts on empty server
		 */
		AsyncClient(boost::asio::io_context &n_io_context, const std::string &n_server, const boost::uint16_t n_port = 6379);

		virtual ~AsyncClient() noexcept;

		/*! connect right away
			@note if already connected, will re-connect
			@throw network_error on cannot resolve host name
		 */
		void connect();

		void set(const std::string &n_name, const std::string   &n_value);
		void set(const std::string &n_name, const boost::int64_t n_value);

	private:

		friend class MRedisTCPConnection;




		boost::asio::io_context             &m_io_context;
		const std::string                    m_server;         //!< if tcp, is set to server
		std::unique_ptr<MRedisTCPConnection> m_connection;

		// Each client
};

}
}

