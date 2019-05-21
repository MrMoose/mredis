
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisConnection.hpp"
#include "AsyncClient.hpp"
#include "MRedisCommands.hpp"

#include "tools/Log.hpp"
#include "tools/Assert.hpp"

#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <boost/algorithm/string.hpp>

namespace moose {
namespace mredis {

using boost::asio::steady_timer;
using namespace moose::tools;
namespace asio = boost::asio;
namespace ip = asio::ip;

MRedisConnection::MRedisConnection(AsyncClient &n_parent)
		: m_parent{ n_parent }
		, m_socket{ n_parent.io_context() }
		, m_send_retry_timer{ n_parent.io_context() }
		, m_receive_retry_timer{ n_parent.io_context() }
		, m_connect_timeout{ n_parent.io_context() }
// #no_timeouts disabled
		, m_read_timeout{ n_parent.io_context() }
		, m_write_timeout{ n_parent.io_context() }
		, m_buffer_busy{ false }
		, m_status{ Status::Disconnected } {

}

MRedisConnection::~MRedisConnection() noexcept {

}

void MRedisConnection::connect(const std::string &n_server, const boost::uint16_t n_port) {

	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), debug) << "Connecting to TCP redis server on " << n_server << ":" << n_port;

	m_server_name = n_server;
	m_server_port = n_port;

	const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

	m_status = Status::Connecting;

	// Set a deadline for the connect operation.
	m_connect_timeout.expires_after(std::chrono::seconds(MREDIS_CONNECT_TIMEOUT));
	// Initially start the timeout handlers.
	m_connect_timeout.async_wait([this](const boost::system::error_code &n_error) { this->check_connect_deadline(n_error); });

	ip::tcp::resolver resolver(m_parent.io_context());
	ip::tcp::resolver::query query(n_server, boost::lexical_cast<std::string>(n_port), ip::tcp::resolver::query::numeric_service);
	ip::tcp::resolver::results_type resolved_endpoints = resolver.resolve(query);
	if (resolved_endpoints.empty()) {
		m_connect_timeout.cancel();
		BOOST_THROW_EXCEPTION(network_error() << error_message("Cannot resolve host name") << error_argument(n_server));
	}

	// I believe connect() should be sync so I catch the result in here.
	promised_response_ptr promise{ std::make_shared<promised_response>() };
	future_response res = promise->get_future(); 

	asio::async_connect(m_socket, resolved_endpoints,
		[this, promise, n_server](const boost::system::error_code &n_errc, const ip::tcp::endpoint &n_endpoint) {

			// cancel the connection timeout
			m_connect_timeout.cancel();

			if (n_errc) {
				BOOST_LOG_SEV(logger(), warning) << "Could not connect to redis server '" << n_server << "': " << n_errc.message();
				stop();

				promise->set_exception(redis_error() << error_message("Could not connect") 
						<< error_argument(n_server) << error_code(n_errc));
				return;
			}

			// The async_connect() function automatically opens the socket at the start
			// of the asynchronous operation. If the socket is closed at this time then
			// the timeout handler must have run first.
			if (!m_socket.is_open()) {
				std::cout << "Sync connect timed out" << std::endl;

				// Example code tried to connect to other endpoints like this:
				//	start_connect(++endpoint_iter);
				// I will not do this now and just throw an error
				stop();

				promise->set_exception(redis_error() << error_message("Connection timeout"));
				return;
			}

			// send a ping to say hello. Only one ping though, Vassily
			{
#ifdef _WIN32
				std::ostream os(&m_streambuf, std::ostream::binary);
#else
				std::ostream os(&m_streambuf);
#endif
				format_ping(os);
			}

			// put a wait handler into the 
			m_outstanding.emplace_back([promise] (const RedisMessage &n_response) {
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

// #no_timeouts disabled
					m_read_timeout.async_wait([this](const boost::system::error_code &n_error) { this->check_read_deadline(n_error); });
			});
	});

	// Wait for slightly longer than the actual timeout would be, so the deadline timer can kill the connection,
	// which would cause the get() to throw, rather than enforcing it here and having dangling stuff
	if (res.wait_for(boost::chrono::seconds(MREDIS_CONNECT_TIMEOUT + 2)) == boost::future_status::timeout) {
		BOOST_LOG_SEV(logger(), error) << "Connection promise was not set within connection timeout parameters. This is a bug in mredis";
		stop();

		BOOST_THROW_EXCEPTION(network_error() << error_message("Connection timed out on promise. This is a bug"));
	}

	// And wait for the callback to call the future. This will throw on connection or read timeout
	RedisMessage r = res.get();

	if (!is_string(r)) {
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
	const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

	BOOST_LOG_SEV(logger(), normal) << "Connected to redis in " << std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count() << "ms";
}

void MRedisConnection::async_connect(const std::string &n_server, const boost::uint16_t n_port, std::shared_ptr<boost::promise<bool> > n_ret) {
	
	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), debug) << "Async connecting to TCP redis server on " << n_server << ":" << n_port;

	const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

	// Set a deadline for the connect operation.
	m_connect_timeout.expires_after(std::chrono::seconds(MREDIS_CONNECT_TIMEOUT));

	m_status = Status::Connecting;

	ip::tcp::resolver resolver(m_parent.io_context());
	ip::tcp::resolver::query query(n_server, boost::lexical_cast<std::string>(n_port), ip::tcp::resolver::query::numeric_service);
	ip::tcp::resolver::results_type resolved_endpoints = resolver.resolve(query);
	if (resolved_endpoints.empty()) {
		n_ret->set_exception(redis_error() << error_message("Cannot resolve host name") << error_argument(n_server));
		return;
	}

	asio::async_connect(m_socket, resolved_endpoints,
		[this, n_ret, n_server, start](const boost::system::error_code &n_errc, const ip::tcp::endpoint &n_endpoint) {

			// cancel the connection timeout
			boost::system::error_code ec{ boost::asio::error::operation_aborted };
			m_connect_timeout.cancel(ec);

			if (n_errc) {
				BOOST_LOG_SEV(logger(), warning) << "Could not connect to redis server '" << n_server << "': " << n_errc.message();
				stop();

				n_ret->set_exception(redis_error() << error_message("Could not connect")
						<< error_argument(n_server) << error_code(n_errc));
				return;
			}

			// The async_connect() function automatically opens the socket at the start
			// of the asynchronous operation. If the socket is closed at this time then
			// the timeout handler must have run first.
			if (!m_socket.is_open()) {
				std::cout << "Async connect timed out" << std::endl;

				// Example code tried to connect to other endpoints like this:
				//	start_connect(++endpoint_iter);
				// I will not do this now and just throw an error

				n_ret->set_exception(redis_error() << error_message("Connection timeout"));
				return;
			}

			// send a ping to say hello. Only one ping though, Vassily
			{
				std::ostream os(&m_streambuf);
				format_ping(os);
			}

			// Put a callback into the expected responses queue to know what we do when ping returns
			m_outstanding.emplace_back([this, n_ret, start](const RedisMessage &n_response) {

				if (is_error(n_response)) {
					n_ret->set_exception(boost::get<redis_error>(n_response));
					return;
				}

				if (!is_string(n_response) || !boost::algorithm::equals(boost::get<std::string>(n_response), "PONG")) {
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

// #no_timeouts disabled
					m_read_timeout.async_wait([this](const boost::system::error_code &n_error) { this->check_read_deadline(n_error); });
			});
	});

	// Initially start the timeout handlers. (Expiry already set)
	m_connect_timeout.async_wait([this](const boost::system::error_code &n_error) { this->check_connect_deadline(n_error); });
}


void MRedisConnection::async_reconnect() {

	BOOST_LOG_FUNCTION();
	BOOST_LOG_SEV(logger(), debug) << "Reconnecting to TCP redis server on " << m_server_name;

	// #moep delete me or assert when stuff works
	if (m_status != Status::ShutdownReconnect) {
		BOOST_LOG_SEV(logger(), error) << "not in reconnect state";
		return;
	}

	const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

	// Re-initialize the socket as I expect shutdown_reconnect() to leave it closed
	m_socket = boost::asio::ip::tcp::socket{ m_parent.io_context() };

	// Set a deadline for the connect operation.
	m_connect_timeout.expires_after(std::chrono::seconds(MREDIS_CONNECT_TIMEOUT));

	m_status = Status::Connecting;

	ip::tcp::resolver resolver(m_parent.io_context());
	ip::tcp::resolver::query query(m_server_name, std::to_string(m_server_port), ip::tcp::resolver::query::numeric_service);
	ip::tcp::resolver::results_type resolved_endpoints = resolver.resolve(query);
	if (resolved_endpoints.empty()) {
		BOOST_LOG_SEV(logger(), warning) << "Could not resolve redis endpoints";
		boost::system::error_code ignored_error;
		m_socket.close(ignored_error);
		asio::post(m_parent.io_context(), [this] { this->async_reconnect(); });
		return;
	}

	// So, what is the plan? I am assuming I was called by send_outstanding, which means there's stuff
	// to be sent but no connection to send it through.
	// I will continuously try to reconnect until the connection succeeds. When it does, I hand back
	// over to send_outstanding
	asio::async_connect(m_socket, resolved_endpoints,
		[this, start](const boost::system::error_code &n_errc, const ip::tcp::endpoint &n_endpoint) {

			m_connect_timeout.cancel();

			// If we encountered an error, we retry too but delete the socket first
			if (n_errc) {
				BOOST_LOG_SEV(logger(), warning) << "Could not reconnect to redis server: " << n_errc.message();
				boost::system::error_code ignored_error;
				m_socket.close(ignored_error);
	
				async_reconnect();
				return;
			}

			// The async_connect() function automatically opens the socket at the start
			// of the asynchronous operation. If the socket is closed at this time then
			// the timeout handler must have run first.
			if (!m_socket.is_open()) {
				// This is a condition I don't really expect but feat as remains of earlier impl
				BOOST_LOG_SEV(logger(), error) << "Could not reconnect to redis server: " << n_errc.message();
				boost::system::error_code ignored_error;
				m_socket.close(ignored_error);

				async_reconnect();
				return;
			}

			m_status = Status::Pushing;
			const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

			BOOST_LOG_SEV(logger(), normal) << "Reconnected to redis in " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms";

			// Soooo, I suppose I'm connected to the same endpoint as before. No ping or anything
			// I'll start send_outstanding, assuming that this is what we came here for
			send_outstanding_requests();


// #no_timeouts disabled
			m_read_timeout.async_wait([this](const boost::system::error_code &n_error) { this->check_read_deadline(n_error); });

	});

	// Initially start the timeout handlers. (Expiry for connect already set)
	m_connect_timeout.async_wait([this](const boost::system::error_code &n_error) {

		if (this->m_status == Status::ShuttingDown || this->m_status == Status::Shutdown) {
			BOOST_LOG_SEV(logger(), debug) << "Shutting down, reconnect timeout ignored";
			return;
		}

		// Check whether the deadline has passed. 
		if (m_connect_timeout.expiry() <= steady_timer::clock_type::now()) {
			// The deadline has passed. The socket is closed so that any outstanding
			// asynchronous operations are cancelled.
			BOOST_LOG_SEV(logger(), debug) << "Reconnect timeout, killing socket";

			async_reconnect();

			// There is no longer an active deadline. The expiry is set to the
			// maximum time point so that the actor takes no action until a new
			// deadline is set.
			m_connect_timeout.expires_at(steady_timer::time_point::max());
		} else {
			std::cout << "deadline not passed, timeout ignored" << std::endl;
		}
	});
}







void MRedisConnection::stop() noexcept {

	BOOST_LOG_FUNCTION();
	
	if (m_status >= Status::ShuttingDown) {
		// We may already be shutting down
		return;
	}

	BOOST_LOG_SEV(logger(), normal) << "MRedis TCP connection now shutting down";

	m_status = Status::ShuttingDown;

	// requests we haven't sent yet timeout immediately
	for (const mrequest &r : m_requests_not_sent) {
	
		asio::post(m_parent.io_context(), [cb{ r.m_callback }] {
			try {
				redis_error err;
				BOOST_LOG_SEV(logger(), normal) << "Stop aborting remaining unsent handler";
				err.set_server_message("unsent handler aborted");
				cb(err);
			} catch (const std::exception &sex) {
				BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused exception: " << sex.what();
			} catch (...) {
				BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused unknown exception";
			}
		});
	}	
	
	m_requests_not_sent.clear();

	// Post remaining handlers into the owning parent's io_service to be executed with a
	// timeout exception
	// I'd much rather salvage them and use them in a new connection but this has to wait
	// because that would imply keeping the prepare functions as well, so we can send them again
	// after the reconnect
	for (const Callback &cb : m_outstanding) {
		
		asio::post(m_parent.io_context(), [cb] {	
			try {
				redis_error err;
				BOOST_LOG_SEV(logger(), normal) << "Stop aborting remaining handler";
				err.set_server_message("handler aborted");
				cb(err);
			} catch (const std::exception &sex) {
				BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused exception: " << sex.what();
			} catch (...) {
				BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused unknown exception";
			}
		});
	}

	m_outstanding.clear();

	m_send_retry_timer.cancel();
	m_receive_retry_timer.cancel();
	m_connect_timeout.cancel();

	// Closing the socket will cause read_response to return its handler with an error
	boost::system::error_code ignored_error;
	m_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored_error);
	m_socket.close(ignored_error);

	// All remaining handlers will be posted for execution with an error into the io_service


	m_status = Status::Shutdown;




}


void MRedisConnection::shutdown_reconnect() noexcept {

	BOOST_LOG_FUNCTION();

	if (m_status == Status::ShuttingDown || m_status == Status::Shutdown) {
		// We may already be shutting down
		return;
	}

	// Leave it in this status
	m_status = Status::ShutdownReconnect;

	BOOST_LOG_SEV(logger(), normal) << "MRedis TCP connection now shutting down to reconnect";

	// requests we haven't sent yet timeout immediately
	for (const mrequest &r : m_requests_not_sent) {
		try {
			redis_error err;
			BOOST_LOG_SEV(logger(), normal) << "Reconnect aborting remaining unsent handler";
			err.set_server_message("unsent handler aborted");
			r.m_callback(err);
		} catch (const std::exception &sex) {
			BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused exception: " << sex.what();
		} catch (...) {
			BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused unknown exception";
		}
	}

	m_requests_not_sent.clear();

	// Post remaining handlers into the owning parent's io_service to be executed with a
	// timeout exception
	// I'd much rather salvage them and use them in a new connection but this has to wait
	// because that would imply keeping the prepare functions as well, so we can send them again
	// after the reconnect
	for (const Callback &cb : m_outstanding) {
		try {
			redis_error err;
			BOOST_LOG_SEV(logger(), normal) << "Reconnect aborting remaining handler";
			err.set_server_message("handler aborted");
			cb(err);
		} catch (const std::exception &sex) {
			BOOST_LOG_SEV(logger(), warning) << "Aborting client callback caused exception: " << sex.what();
		} catch (...) {
			BOOST_LOG_SEV(logger(), error) << "Aborting client callback caused unknown exception";
		}	
	}

	// after executing the callbacks with an error, delete them.
	m_outstanding.clear();

	boost::system::error_code ec{ boost::asio::error::operation_aborted };
	m_send_retry_timer.cancel(ec);
	m_receive_retry_timer.cancel(ec);
	m_connect_timeout.cancel(ec);

	// Closing the socket will cause read_response to return its handler with an error
	boost::system::error_code ignored_error;
	m_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored_error);
	m_socket.close(ignored_error);

	// which is why we poll it now. We are in io_service's thread
	m_parent.io_context().poll();

	// Whatever is left in the streambuf is BS now. I want to clear it. There is no described 
	// way to do this though, so I resort to take all the input sequence
	m_streambuf.consume(m_streambuf.size());

	BOOST_LOG_SEV(logger(), normal) << "MRedis TCP connection now in shutdown status, ready to reconnect";
}




void MRedisConnection::send(std::function<void(std::ostream &n_os)> &&n_prepare, Callback &&n_callback) noexcept {

	{
		// MOEP! Introduce lockfree queue!

		
		mrequest req{ std::move(n_prepare) , std::move(n_callback) };
		boost::unique_lock<boost::mutex> slock(m_request_queue_lock);
		m_requests_not_sent.emplace_back(std::move(req));
	}


	m_parent.io_context().post([this]() { this->send_outstanding_requests(); });
}

promised_response_ptr MRedisConnection::send(std::function<void(std::ostream &n_os)> &&n_prepare) noexcept {

	// We keep ownership over this promise in the mrequest object
	promised_response_ptr promise(std::make_shared<promised_response>());

	{
		// Assemble a request object which will live until we have sent the request (via prepare function)
		// after which we discard the object and only keep the promised answer
		mrequest req{
			// Prepare function (will issue command on stream)
			std::move(n_prepare),

			// Callback will be called once the connection receives a response to the call
			[promise](const RedisMessage &n_response) {

				// A received (nested) error is re-thrown as a real exception 
				// that will rise out of the future.
				if (is_error(n_response)) {
					promise->set_exception(boost::get<redis_error>(n_response));
				} else {
					promise->set_value(n_response);
				}
			} 
		};

		// #moep Introduce lockfree queue!


		boost::unique_lock<boost::mutex> slock(m_request_queue_lock);
		m_requests_not_sent.emplace_back(std::move(req));
	}

	m_parent.io_context().post([this]() { this->send_outstanding_requests(); });

	return promise;
}

void MRedisConnection::send_outstanding_requests() noexcept {

	try {
		// if the streambuf is still in use we have to wait
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {

			// If we already have a retry timer set, don't do it again
			if (m_send_retry_timer.expiry() < steady_timer::clock_type::time_point::max()) {

				// continue waiting for the handler to invoke
				return;
			}

			m_send_retry_timer.expires_after(asio::chrono::milliseconds(1));
			m_send_retry_timer.async_wait(
				[this](const boost::system::error_code &n_err) {

					// This logically resets the timer to 'unset'
					m_send_retry_timer.expires_at(steady_timer::clock_type::time_point::max());

					if (!n_err && this->m_status == Status::Pushing) {
						this->send_outstanding_requests();
					}
				});
			return;
		}

		if (m_status == Status::ShutdownReconnect) {
			// we were shutdown by some error condition and try a reconnect


			BOOST_LOG_SEV(logger(), debug) << "Incoming command triggered reconnect";
			async_reconnect();


			return;
		}




		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		{
#ifdef _WIN32
			std::ostream os(&m_streambuf, std::ostream::binary);
#else
			std::ostream os(&m_streambuf);
#endif

//			BOOST_LOG_SEV(logger(), debug) << "Sending " << m_requests_not_sent.size() << " outstanding requests";
			
			// MOEP! Please, lockfree! 
			boost::unique_lock<boost::mutex> slock(m_request_queue_lock);

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

void MRedisConnection::read_response() noexcept {

	if (m_status >= Status::ShuttingDown) {
		return;
	}

	try {
		// if the streambuf is in use we cannot push data into it
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {

			// If we already have a retry timer set, don't do it again
			if (m_receive_retry_timer.expiry() < steady_timer::clock_type::time_point::max()) {
				
				// continue waiting for the handler to invoke
				return;
			}

			// I will only post a wait handler if the queue of outstanding 
			// responses is not empty to avoid having multiple in flight
			if (!m_outstanding.empty()) {
				m_receive_retry_timer.expires_after(asio::chrono::milliseconds(1));
				m_receive_retry_timer.async_wait(
					[this](const boost::system::error_code &n_err) {

						// This logically resets the timer to 'unset'
						m_receive_retry_timer.expires_at(steady_timer::clock_type::time_point::max());

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
		while (!m_outstanding.empty() && m_streambuf.size()) {
		
			std::istream is(&m_streambuf);
			RedisMessage r;
				
			// As long as we can parse messages from our stream, continue to do so.
			bool success = parse_from_stream(is, r);
			if (success) {
				// call the callback which was stored along with the request
				m_outstanding.front()(r);

				// remove the callback
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
//		BOOST_LOG_SEV(logger(), debug) << "Reading more response";
	

// #no_timeouts disabled
		m_read_timeout.expires_after(std::chrono::seconds(MREDIS_READ_TIMEOUT));

		// read one response and evaluate
		asio::async_read(m_socket, m_streambuf, asio::transfer_at_least(1),
			[this](const boost::system::error_code n_errc, const std::size_t) {
				
				// let the timeout handler know we have read the response we're expecting
// #no_timeouts disabled
				m_read_timeout.expires_at(steady_timer::time_point::max());

				if (handle_error(n_errc, "reading response")) {
					m_buffer_busy = false;
					BOOST_LOG_SEV(logger(), warning) << "stop connection";
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


bool MRedisConnection::handle_error(const boost::system::error_code n_errc, const char *n_message) const {

	if (!n_errc) {
		return false;
	}

	// Most likely an operation has been canceled due to shutdown
	if (m_status >= Status::ShuttingDown) {
		BOOST_LOG_SEV(logger(), normal) << "Async operation aborted, shutting down: " << n_errc.message();
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

void MRedisConnection::check_connect_deadline(const boost::system::error_code &n_error) {
	
	if (n_error == boost::asio::error::operation_aborted) {
		BOOST_LOG_SEV(logger(), debug) << "Connection timeout cancelled";
		return;
	}

	if (m_status == Status::ShuttingDown || m_status == Status::Shutdown) {
		BOOST_LOG_SEV(logger(), debug) << "Shutting down, connection timeout ignored";
		return;
	}

	// Check whether the deadline has passed. We compare the deadline against
	// the current time since a new asynchronous operation may have moved the
	// deadline before this actor had a chance to run.
	if (m_connect_timeout.expiry() <= steady_timer::clock_type::now()) {
		// The deadline has passed. The socket is closed so that any outstanding
		// asynchronous operations are cancelled.
		BOOST_LOG_SEV(logger(), warning) << "Connect timeout, killing connection";
		stop();
	}
}

void MRedisConnection::check_read_deadline(const boost::system::error_code &n_error) {

	if (m_status == Status::ShuttingDown || m_status == Status::Shutdown) {
		BOOST_LOG_SEV(logger(), debug) << "Shutting down, read timeout ignored";
		return;
	}

	// Check whether the deadline has passed. We compare the deadline against
	// the current time since a new asynchronous operation may have moved the
	// deadline before this actor had a chance to run.
	if (m_read_timeout.expiry() <= steady_timer::clock_type::now()) {
		// The deadline has passed. The socket is closed so that any outstanding
		// asynchronous operations are cancelled.
		BOOST_LOG_SEV(logger(), normal) << "Read timeout, killing connection for reconnect...";

		shutdown_reconnect();

		// There is no longer an active deadline. The expiry is set to the
		// maximum time point so that the actor takes no action until a new
		// deadline is set.
		m_read_timeout.expires_at(steady_timer::time_point::max());
	} else {
//		BOOST_LOG_SEV(logger(), debug) << "Read deadline not passed, timeout ignored";
	}

	// Put the actor back to sleep.
	m_read_timeout.async_wait([this](const boost::system::error_code &e) { this->check_read_deadline(e); });
}



}
}
