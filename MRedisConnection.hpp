
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisConnection.hpp"
#include "RESP.hpp"

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>

#include <functional>
#include <string>

#include <deque>

namespace moose {
namespace mredis {

class AsyncClient;

class MRedisConnection {

	public:
		//! all timeouts in seconds
		enum { MREDIS_CONNECT_TIMEOUT =  2 };
		enum { MREDIS_READ_TIMEOUT    =  5 };   // make that 10
		enum { MREDIS_WRITE_TIMEOUT   =  5 };

		MRedisConnection(AsyncClient &n_parent);
		MRedisConnection(const MRedisConnection &) = delete;
		virtual ~MRedisConnection() noexcept;
		MRedisConnection &operator=(const MRedisConnection &) = delete;


		//! This blocks until connected or throws on error
		void connect(const std::string &n_server, const boost::uint16_t n_port = 6379);

		//! This doesn't block and sets promise upon done
		void async_connect(const std::string &n_server, const boost::uint16_t n_port, std::shared_ptr<boost::promise<bool> > n_ret);


		//! This doesn't block and sets promise upon done
		void async_reconnect();




		/*! @brief shut the connection down.
			There may still be cancelled handlers on the io_service for it. Make sure you poll before 
				actually destroying the connections

			@param n_reconnect if true, the connection will go to state Status::ShutdownReconnect
				and attempt to re-connect at the next send() 
		 */
		virtual void stop() noexcept;

		/*! @brief send an unknown command that can be filled by the caller via n_prepare		
		 */
		void send(std::function<void(std::ostream &n_os)> &&n_prepare, Callback &&n_callback) noexcept;

		/*! @brief send an unknown command that can be filled by the caller via n_prepare	
		 */
		promised_response_ptr send(std::function<void(std::ostream &n_os)> &&n_prepare) noexcept;

		//! connection lifecycle
		enum class Status {
			Disconnected = 0,
			Connecting,
			Connected,
			Pushing,           // normal mode connection
			Pubsub,            // pubsub connection
			ShuttingDown,
			ShutdownReconnect, // down because of error or timeout, try to reconnect 
			Shutdown           // down and out
		};

	protected:
		//! assume there are some and go send
		void send_outstanding_requests() noexcept;

		void read_response() noexcept;

		//! when handling error conditions after async ops, use this to save some lines
		//! @return true when error should cause closing of the connection
		bool handle_error(const boost::system::error_code n_errc, const char *n_message) const;

		//! yeah, this needs refactoring. I don't need two functions for that
		void check_connect_deadline(const boost::system::error_code &n_error);

		void check_read_deadline(const boost::system::error_code &n_error);

		/*! @brief shut the connection down and try to reconnect until done.
			must be called by the connection itself, in io_service's thread


		 */
		virtual void shutdown_reconnect() noexcept;



		AsyncClient                   &m_parent;
		std::string                    m_server_name;
		boost::uint16_t                m_server_port;
		boost::asio::ip::tcp::socket   m_socket;              //!< Socket for the connection.	
		boost::asio::streambuf         m_streambuf;           //!< use for reading and writing both


		boost::asio::steady_timer      m_send_retry_timer;    //!< when buffer is in use for sending, retry after a few micros
		boost::asio::steady_timer      m_receive_retry_timer; //!< when buffer is in use for receiving, retry after a few micros
		boost::asio::steady_timer      m_connect_timeout;     //!< we try to adopt a timeout concept but since we are in a 

		boost::asio::steady_timer      m_read_timeout;        //!< pipelining connection, we always have two concurrent timeouts
		boost::asio::steady_timer      m_write_timeout;


		boost::mutex                   m_request_queue_lock;
		
		std::deque<mrequest>           m_requests_not_sent;
		std::deque<Callback>           m_outstanding;         //!< callbacks that have not been resolved yet. We are waiting for an answer

		bool                           m_buffer_busy;         //!< streambuf in use

		Status                         m_status;              //!< tell where we are in our workflow
		std::string                    m_serialized_request;  //!< whatever we have to say. I optimize for 1024 bytes
};

}
}
