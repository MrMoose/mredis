
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisTCPConnection.hpp"
#include "RESP.hpp"

#include <boost/asio.hpp>

#include <functional>
#include <string>

namespace moose {
namespace mredis {

class AsyncClient;

class MRedisTCPConnection {

	public:
		MRedisTCPConnection(AsyncClient &n_parent);
		MRedisTCPConnection(const MRedisTCPConnection &) = delete;
		virtual ~MRedisTCPConnection() noexcept;
		MRedisTCPConnection &operator=(const MRedisTCPConnection &) = delete;

		void connect(const std::string &n_server, const boost::uint16_t n_port = 6379);
		void stop() noexcept;

		//! send an unknown command that can be filled by the caller via n_prepare
		void send_command(std::function<void(std::ostream &n_os)> &&n_prepare, std::function<void(const RESPonse &)> &&n_callback) noexcept;

	private:

		void send_ping();

		void read_pong();

		void read_response(std::function<void(const RESPonse &)> &&n_callback);

		//! when handling error conditions after async ops, use this to save some lines
		//! @return true when error should cause closing of the connection
		bool handle_error(const boost::system::error_code n_errc, const char *n_message) const;

		//! connection lifecycle
		enum class Status {
			Disconnected = 0,
			Connecting,
			Connected,
			Pushing,
			Pulling,
			Shutdown
		};

		AsyncClient                 &m_parent;
		boost::asio::ip::tcp::socket m_socket;             //!< Socket for the connection.	
		boost::asio::streambuf       m_streambuf;          //!< use for reading packets and headers on upstream
		boost::asio::steady_timer    m_retry_timer;        //!< when buffer is in use, retry after a few micros
		bool                         m_sending;            //!< streambuf in use
		Status                       m_status;             //!< tell where we are in our workflow
		std::string                  m_serialized_request; //!< whatever we have to say. I optimize for 1024 bytes
};

}
}
