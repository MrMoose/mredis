
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisTCPConnection.hpp"
#include "RESP.hpp"

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>

#include <functional>
#include <string>

#include <deque>

namespace moose {
namespace mredis {

class AsyncClient;

class MRedisTCPConnection {

	public:
		MRedisTCPConnection(AsyncClient &n_parent);
		MRedisTCPConnection(const MRedisTCPConnection &) = delete;
		virtual ~MRedisTCPConnection() noexcept;
		MRedisTCPConnection &operator=(const MRedisTCPConnection &) = delete;

		//! This blocks until connected or throws on error
		void connect(const std::string &n_server, const boost::uint16_t n_port = 6379);

		//! This doesn't block and sets promise upon done
		void async_connect(const std::string &n_server, const boost::uint16_t n_port, boost::shared_ptr<boost::promise<bool> > n_ret);


		/*! @brief shut the connection down.
			There may still be cancelled handlers on the io_service for it. Make sure you poll before 
				actually destroying the connections
		 */
		void stop() noexcept;

		/*! send an unknown command that can be filled by the caller via n_prepare
			must be called from io_service thread
		 */
		void send(std::function<void(std::ostream &n_os)> &&n_prepare, Callback &&n_callback) noexcept;

		/*! send an unknown command that can be filled by the caller via n_prepare
			must be called from io_service thread
		 */
		promised_response_ptr send(std::function<void(std::ostream &n_os)> &&n_prepare) noexcept;

	private:
		/*! send an unknown command that can be filled by the caller via n_prepare
			must be called from io_service thread

			@note deprecated, i try to be queue only

		 */
		void send_command_orig(std::function<void(std::ostream &n_os)> &&n_prepare, Callback &&n_callback) noexcept;

		//! assume there are some and go send
		void send_outstanding_requests() noexcept;

		void read_response() noexcept;


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
			ShuttingDown,
			Shutdown
		};

		AsyncClient                 &m_parent;
		boost::asio::ip::tcp::socket m_socket;              //!< Socket for the connection.	
		boost::asio::streambuf       m_streambuf;           //!< use for reading packets and headers on upstream
		boost::asio::steady_timer    m_send_retry_timer;    //!< when buffer is in use for sending, retry after a few micros
		boost::asio::steady_timer    m_receive_retry_timer; //!< when buffer is in use for receiving, retry after a few micros
		
		boost::mutex                 m_request_queue_lock;
		std::deque<mrequest>         m_requests_not_sent;

		std::deque<Callback>         m_outstanding;

		bool                         m_buffer_busy;        //!< streambuf in use
		Status                       m_status;             //!< tell where we are in our workflow
		std::string                  m_serialized_request; //!< whatever we have to say. I optimize for 1024 bytes
};

}
}
