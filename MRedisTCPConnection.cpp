
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisTCPConnection.hpp"
#include "AsyncClient.hpp"
#include "RESP.hpp"

#include "tools/Log.hpp"
#include "tools/Assert.hpp"

#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <boost/algorithm/string.hpp>

namespace moose {
namespace mredis {

using namespace moose::tools;
namespace asio = boost::asio;
namespace ip = asio::ip;

MRedisTCPConnection::MRedisTCPConnection(AsyncClient &n_parent)
		: m_parent(n_parent)
		, m_socket(n_parent.m_io_context)
		, m_send_retry_timer(n_parent.m_io_context)
		, m_receive_retry_timer(n_parent.m_io_context)
		, m_buffer_busy(false)
		, m_status(Status::Disconnected) {

}

MRedisTCPConnection::~MRedisTCPConnection() noexcept {
	
	m_send_retry_timer.cancel();
	m_receive_retry_timer.cancel();
}

void MRedisTCPConnection::connect(const std::string &n_server, const boost::uint16_t n_port) {

	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), debug) << "Connecting to TCP redis server on " << n_server << ":" << n_port;

	m_status = Status::Connecting;

	ip::tcp::resolver resolver(m_parent.m_io_context);
	ip::tcp::resolver::query query(n_server, boost::lexical_cast<std::string>(n_port), ip::tcp::resolver::query::numeric_service);
	ip::tcp::resolver::results_type resolved_endpoints = resolver.resolve(query);
	if (resolved_endpoints.empty()) {
		BOOST_THROW_EXCEPTION(network_error() << error_message("Cannot resolve host name") << error_argument(n_server));
	}

	asio::async_connect(m_socket, resolved_endpoints,
		[this, n_server](const boost::system::error_code &n_errc, const ip::tcp::endpoint &n_endpoint) {

			if (n_errc) {
				BOOST_LOG_SEV(logger(), warning) << "Could not connect to redis server '" << n_server << "': " << n_errc.message();
				stop();
				return;
			}

			// send a ping to say hello. Only one ping though, Vassily
			send_ping();
	});
}

void MRedisTCPConnection::stop() noexcept {

	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), normal) << "MRedis TCP connection shutting down";

	m_send_retry_timer.cancel();
	m_receive_retry_timer.cancel();

	m_status = Status::Shutdown;

	boost::system::error_code ec;
	m_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
	if (ec) {
		BOOST_LOG_SEV(logger(), warning) << "Error shutting down Redis connection: " << ec.message() << " .. don't worry about that.";
	}

	m_socket.close(ec);
	if (ec) {
		BOOST_LOG_SEV(logger(), warning) << "Error closing down Redis connection: " << ec.message() << " .. don't worry about that.";
	}
}

void MRedisTCPConnection::send_ping() {

	std::ostream os(&m_streambuf);
	format_ping(os);

	// and send
	asio::async_write(m_socket, m_streambuf,
		[this](const boost::system::error_code n_errc, const std::size_t n_bytes_sent) {

			if (handle_error(n_errc, "writing initial ping")) { stop(); return; }

			read_pong();

			m_status = Status::Connected;
			BOOST_LOG_SEV(logger(), normal) << "Connected to redis via TCP";
	});
}


void MRedisTCPConnection::read_pong() {

	read_response([this](const RESPonse &n_response) {

		if (n_response.which() != 1) {
			BOOST_LOG_SEV(logger(), error) << "Server did not pong";
			stop();
			return;
		}

		if (!boost::algorithm::equals(boost::get<std::string>(n_response), "PONG")) {
			BOOST_LOG_SEV(logger(), error) << "Server did not pong";
			stop();
			return;
		}

		// Normally, connections are pushing. Meaning they send commands actively 
		// as opposed to reading the socket for subscription channel messages
		m_status = Status::Pushing;
		BOOST_LOG_SEV(logger(), normal) << "Connected to redis";
	});
}

void MRedisTCPConnection::read_response(std::function<void(const RESPonse &)> &&n_callback) {

	// read one response and evaluate
	asio::async_read_until(m_socket, m_streambuf, "\r\n",
		[this, n_callback](const boost::system::error_code n_errc, const std::size_t) {

			if (handle_error(n_errc, "reading response")) { stop(); return; }

			std::istream is(&m_streambuf);

			RESPonse r = parse_one(is);
			n_callback(r);
	});
}

void MRedisTCPConnection::send_command(std::function<void(std::ostream &n_os)> &&n_prepare, std::function<void(const RESPonse &)> &&n_callback) noexcept {
	
	MOOSE_ASSERT_MSG(n_prepare, "stream preparation function must be given");

	if (m_status != Status::Pushing) {
		BOOST_LOG_SEV(logger(), warning) << "Cannot push command into this connection. It is not in state pushing";
		return;
	}

	try {
		// if the streambuf is in use we cannot push data into it
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {
			mrequest req;
			req.m_prepare = std::move(n_prepare);
			req.m_callback = std::move(n_callback);
			m_requests_not_sent.emplace_back(std::move(req));

			// I will only post a wait handler if the queue of outstanding 
			// requests is empty to avoid having multiple in flight
			if (m_requests_not_sent.size() > 0) {
				m_send_retry_timer.expires_after(asio::chrono::milliseconds(1));
				m_send_retry_timer.async_wait(
					[this](const boost::system::error_code &n_err) {

						if (!n_err && this->m_status == Status::Pushing) {
							this->send_outstanding_requests();
						}
					});
			}
			return;
		}

		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		{
			std::ostream os(&m_streambuf);

			// See if we have unsent requests and stream them first in the order they came in
			for (mrequest &req : m_requests_not_sent) {
				req.m_prepare(os);
				m_outstanding.emplace_back(req.m_callback);
			}

			m_requests_not_sent.clear();

			// The caller told us what he wants to put into the command
			n_prepare(os);
			m_outstanding.emplace_back(n_callback);
		}

		// send the content of the streambuf to redis
		asio::async_write(m_socket, m_streambuf,
			[this, n_callback](const boost::system::error_code n_errc, const std::size_t n_bytes_sent) {

				m_buffer_busy = false;
				if (handle_error(n_errc, "sending command(s) to server")) {
					stop();
					return;
				}

				read_response();
		});

	} catch (const boost::system::system_error &serr) {
		// Very likely we failed to set a retry timer. Doesn't matter. We still wake up as the next command comes in
		BOOST_LOG_SEV(logger(), warning) << "Error sending command: " << boost::diagnostic_information(serr);
	} catch (const tools::moose_error &merr) {
		// Not expected at all
		BOOST_LOG_SEV(logger(), warning) << "Error sending command: " << boost::diagnostic_information(merr);
	} catch (const std::exception &sex) {
		// The caller didn't provide callbacks that were nothrow
		BOOST_LOG_SEV(logger(), error) << "Unexpected exception in send_command(): " << boost::diagnostic_information(sex);
	}
}


void MRedisTCPConnection::send_outstanding_requests() noexcept {

	try {
		// if the streambuf is still in use we have to wait
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {		
			m_send_retry_timer.expires_after(asio::chrono::milliseconds(1));
			m_send_retry_timer.async_wait(
				[this](const boost::system::error_code &n_err) {

					if (!n_err && this->m_status == Status::Pushing) {
						this->send_outstanding_requests();
					}
				});
			return;
		}

		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		{
			std::ostream os(&m_streambuf);

			// See if we have unsent requests and stream them first in the order they came in
			for (mrequest &req : m_requests_not_sent) {
				req.m_prepare(os);
				m_outstanding.emplace_back(req.m_callback);
			}

			m_requests_not_sent.clear();
		}

		// send the content of the streambuf to redis
		asio::async_write(m_socket, m_streambuf,
			[this](const boost::system::error_code n_errc, const std::size_t n_bytes_sent) {

				m_buffer_busy = false;
				if (handle_error(n_errc, "sending command(s) to server")) {
					stop();
					return;
				}

				read_response();
			});

	} catch (const boost::system::system_error &serr) {
		// Very likely we failed to set a retry timer. Doesn't matter. We still wake up as the next command comes in
		BOOST_LOG_SEV(logger(), warning) << "Error sending command: " << boost::diagnostic_information(serr);
	} catch (const tools::moose_error &merr) {
		// Not expected at all
		BOOST_LOG_SEV(logger(), warning) << "Error sending command: " << boost::diagnostic_information(merr);
	} catch (const std::exception &sex) {
		// The caller didn't provide callbacks that were nothrow
		BOOST_LOG_SEV(logger(), error) << "Unexpected exception in send_command(): " << boost::diagnostic_information(sex);
	}
}

void MRedisTCPConnection::read_response() noexcept {

	try {
		// if the streambuf is in use we cannot push data into it
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {
			
			// I will only post a wait handler if the queue of outstanding 
			// responses is not empty to avoid having multiple in flight
			if (m_outstanding.size() > 0) {
				m_send_retry_timer.expires_after(asio::chrono::milliseconds(1));
				m_send_retry_timer.async_wait(
					[this](const boost::system::error_code &n_err) {

						if (!n_err && this->m_status == Status::Pushing) {
							this->read_response();
						}
				});
			}
			return;
		}
		
		if (m_outstanding.empty()) {
			return;
		}

		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		// read one response and evaluate
		asio::async_read_until(m_socket, m_streambuf, "\r\n",
			[this](const boost::system::error_code n_errc, const std::size_t) {

				if (handle_error(n_errc, "reading response")) {
					m_buffer_busy = false;
					stop();
					return;
				}

				{
					std::istream is(&m_streambuf);
					RESPonse r = parse_one(is);
					m_outstanding.front()(r);
					m_outstanding.pop_front();
				}

				m_buffer_busy = false;
				read_response();
			});
	} catch (const boost::system::system_error &serr) {
		// Very likely we failed to set a retry timer. Doesn't matter. We still wake up as the next command comes in
		BOOST_LOG_SEV(logger(), warning) << "Error reading response: " << boost::diagnostic_information(serr);
	} catch (const tools::moose_error &merr) {
		// Not expected at all
		BOOST_LOG_SEV(logger(), warning) << "Error reading response: " << boost::diagnostic_information(merr);
	} catch (const std::exception &sex) {
		// The caller didn't provide callbacks that were nothrow
		BOOST_LOG_SEV(logger(), error) << "Unexpected exception in read_response(): " << boost::diagnostic_information(sex);
	}
}


bool MRedisTCPConnection::handle_error(const boost::system::error_code n_errc, const char *n_message) const {

	if (!n_errc) {
		return false;
	}

	if (n_errc == boost::asio::error::operation_aborted) {
		BOOST_LOG_SEV(logger(), normal) << "Async operation aborted - " << n_message << ": " << n_errc.message();
		return true;
	}

	if (n_errc == boost::asio::error::broken_pipe) {
		BOOST_LOG_SEV(logger(), warning) << "Server closed connection - " << n_message << ": " << n_errc.message();
		return true;
	}

	BOOST_LOG_SEV(logger(), warning) << "Error " << n_message << ": " << n_errc.message();
	return true;
}



}
}
