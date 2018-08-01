
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

}

void MRedisTCPConnection::connect(const std::string &n_server, const boost::uint16_t n_port) {

	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), debug) << "Connecting to TCP redis server on " << n_server << ":" << n_port;

	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

	m_status = Status::Connecting;

	ip::tcp::resolver resolver(m_parent.m_io_context);
	ip::tcp::resolver::query query(n_server, boost::lexical_cast<std::string>(n_port), ip::tcp::resolver::query::numeric_service);
	ip::tcp::resolver::results_type resolved_endpoints = resolver.resolve(query);
	if (resolved_endpoints.empty()) {
		BOOST_THROW_EXCEPTION(network_error() << error_message("Cannot resolve host name") << error_argument(n_server));
	}

	// I believe connect() should be sync so I catch the result in here.
	promised_response_ptr promise(boost::make_shared<promised_response>());
	future_response res = promise->get_future(); 

	asio::async_connect(m_socket, resolved_endpoints,
		[this, promise, n_server](const boost::system::error_code &n_errc, const ip::tcp::endpoint &n_endpoint) {

			if (n_errc) {
				BOOST_LOG_SEV(logger(), warning) << "Could not connect to redis server '" << n_server << "': " << n_errc.message();
				stop();			
				promise->set_exception(redis_error() << error_message("Could not connect"));
				return;
			}

			// send a ping to say hello. Only one ping though, Vassily
			{
				std::ostream os(&m_streambuf);
				format_ping(os);
			}

			m_outstanding.emplace_back([this, promise](const RESPonse &n_response) {
				promise->set_value(n_response);
			});

			// send the content of the streambuf to redis
			asio::async_write(m_socket, m_streambuf,
				[this](const boost::system::error_code n_errc, const std::size_t n_bytes_sent) {

					if (handle_error(n_errc, "sending ping to server")) {
						stop();
						return;
					}

					read_response();
			});
	});

	RESPonse r = res.get();

	if (r.which() != 1) {
		BOOST_LOG_SEV(logger(), error) << "Server did not pong";
		stop();
		BOOST_THROW_EXCEPTION(network_error() << error_message("Server did not respond to ping"));
	}

	if (!boost::algorithm::equals(boost::get<std::string>(r), "PONG")) {
		BOOST_LOG_SEV(logger(), error) << "Server did not pong";
		stop();
		BOOST_THROW_EXCEPTION(network_error() << error_message("Server did not respond to ping with PONG"));
	}

	// Normally, connections are pushing. Meaning they send commands actively 
	// as opposed to reading the socket for subscription channel messages
	m_status = Status::Pushing;
	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

	BOOST_LOG_SEV(logger(), normal) << "Connected to redis in " << std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count() << "ms";
	return;
}

void MRedisTCPConnection::async_connect(const std::string &n_server, const boost::uint16_t n_port, boost::shared_ptr<boost::promise<bool> > n_ret) {
	
	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), debug) << "Async connecting to TCP redis server on " << n_server << ":" << n_port;

	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

	m_status = Status::Connecting;

	ip::tcp::resolver resolver(m_parent.m_io_context);
	ip::tcp::resolver::query query(n_server, boost::lexical_cast<std::string>(n_port), ip::tcp::resolver::query::numeric_service);
	ip::tcp::resolver::results_type resolved_endpoints = resolver.resolve(query);
	if (resolved_endpoints.empty()) {
		n_ret->set_exception(redis_error() << error_message("Cannot resolve host name") << error_argument(n_server));
		return;
	}

	asio::async_connect(m_socket, resolved_endpoints,
		[this, n_ret, n_server, start](const boost::system::error_code &n_errc, const ip::tcp::endpoint &n_endpoint) {

			if (n_errc) {
				BOOST_LOG_SEV(logger(), warning) << "Could not connect to redis server '" << n_server << "': " << n_errc.message();
				stop();
				n_ret->set_exception(redis_error() << error_message("Could not connect") << error_argument(n_server));
				return;
			}

			// send a ping to say hello. Only one ping though, Vassily
			{
				std::ostream os(&m_streambuf);
				format_ping(os);
			}

			// Put a callback into the expected responses queue to know what we do when ping returns
			m_outstanding.emplace_back([this, n_ret, start](const RESPonse &n_response) {

				if (n_response.which() == 0) {
					n_ret->set_exception(boost::get<redis_error>(n_response));
					return;
				}

				if (n_response.which() != 1 || !boost::algorithm::equals(boost::get<std::string>(n_response), "PONG")) {
					n_ret->set_exception(redis_error() << error_message("Server did not pong"));
					return;
				}

				// Normally, connections are pushing. Meaning they send commands actively 
				// as opposed to reading the socket for subscription channel messages
				m_status = Status::Pushing;
				const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	
				BOOST_LOG_SEV(logger(), normal) << "Connected to redis in " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms";
				n_ret->set_value(true);
			});

			// send the content of the streambuf to redis
			asio::async_write(m_socket, m_streambuf,
				[this, n_ret](const boost::system::error_code n_errc, const std::size_t n_bytes_sent) {

					if (handle_error(n_errc, "sending ping to server")) {
						stop();
						return;
					}

					read_response();
			});
	});

	return;
}

void MRedisTCPConnection::stop() noexcept {

	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), normal) << "MRedis TCP connection shutting down";

	if (m_status == Status::ShuttingDown || m_status == Status::Shutdown) {
		BOOST_LOG_SEV(logger(), warning) << "Please don't stop TCP connection twice";
		return;
	}

	m_status = Status::ShuttingDown;

	m_send_retry_timer.cancel();
	m_receive_retry_timer.cancel();

	boost::system::error_code ec;
	m_socket.close(ec);
	if (ec) {
		BOOST_LOG_SEV(logger(), warning) << "Error closing down Redis connection: " << ec.message() << " .. don't worry about that.";
	}
}

void MRedisTCPConnection::send_command(std::function<void(std::ostream &n_os)> &&n_prepare, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT_MSG(n_prepare, "stream preparation function must be given");

	if (m_status != Status::Pushing) {
		BOOST_LOG_SEV(logger(), warning) << "Cannot push command into this connection. It is not in state pushing";
		return;
	}

	try {
		// if the streambuf is in use we cannot push data into it
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {
			mrequest req{ std::move(n_prepare) , std::move(n_callback) };
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
				m_outstanding.emplace_back(std::move(req.m_callback));
			}

			m_requests_not_sent.clear();

			// The caller told us what he wants to put into the command
			n_prepare(os);
			m_outstanding.emplace_back(std::move(n_callback));
		}

		// send the content of the streambuf to redis
		asio::async_write(m_socket, m_streambuf,
			[this, n_callback](const boost::system::error_code n_errc, const std::size_t n_bytes_sent) {

				m_buffer_busy = false;
				if (handle_error(n_errc, "sending command(s) to server")) {
					stop();
					return;
				}

				if (m_status >= Status::ShuttingDown) {
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

			BOOST_LOG_SEV(logger(), debug) << "Sending " << m_requests_not_sent.size() << " outstanding requests";

			// See if we have unsent requests and stream them first in the order they came in
			for (mrequest &req : m_requests_not_sent) {
				req.m_prepare(os);
				m_outstanding.emplace_back(std::move(req.m_callback));
			}

			m_requests_not_sent.clear();
		}

		// send the content of the streambuf to redis
		asio::async_write(m_socket, m_streambuf,
			[this](const boost::system::error_code n_errc, const std::size_t) {

				m_buffer_busy = false;
				if (handle_error(n_errc, "sending command(s) to server")) {
					stop();
					return;
				}

				if (m_status >= Status::ShuttingDown) {
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

	if (m_status >= Status::ShuttingDown) {
		return;
	}

	try {
		// if the streambuf is in use we cannot push data into it
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {
			// I will only post a wait handler if the queue of outstanding 
			// responses is not empty to avoid having multiple in flight
			if (m_outstanding.size() > 0) {
				m_receive_retry_timer.expires_after(asio::chrono::milliseconds(1));
				m_receive_retry_timer.async_wait(
					[this](const boost::system::error_code &n_err) {

						if (!n_err && this->m_status == Status::Pushing) {
							this->read_response();
						}
				});
			}
			return;
		}
		
		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		// perhaps we already have bytes to read in our streambuf. If so, I parse those first
		while (m_outstanding.size() && m_streambuf.size()) {
		
			std::istream is(&m_streambuf);
			RESPonse r;
				
			// As long as we can parse messages from our stream, continue to do so.
			bool success = parse_from_stream(is, r);
			if (success) {
				m_outstanding.front()(r);
				m_outstanding.pop_front();
			} else {
				break;
			}
		}

		// If there's nothing left to read, exit this strand
		if (m_outstanding.empty()) {
			m_buffer_busy = false;
			return;
		}

		// Otherwise read more from the socket
		BOOST_LOG_SEV(logger(), debug) << "Reading more response";

		// read one response and evaluate
		asio::async_read(m_socket, m_streambuf, asio::transfer_at_least(1),
			[this](const boost::system::error_code n_errc, const std::size_t) {

				if (handle_error(n_errc, "reading response")) {
					m_buffer_busy = false;
					stop();
					return;
				}
				
				m_buffer_busy = false;

				if (m_status >= Status::ShuttingDown) {
					BOOST_LOG_SEV(logger(), normal) << "Shutting down, not reading any more responses. There are " << m_outstanding.size() << " callbacks left";
					return;
				}

				// this is going to parse
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
