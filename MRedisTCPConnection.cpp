
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisTCPConnection.hpp"
#include "AsyncClient.hpp"
#include "RESP.hpp"

#include "tools/Log.hpp"

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
		, m_socket(n_parent.m_io_context) {

}

MRedisTCPConnection::~MRedisTCPConnection() noexcept {

}


void MRedisTCPConnection::connect(const std::string &n_server, const boost::uint16_t n_port) {

	BOOST_LOG_FUNCTION();

	m_status = Status::Connecting;

	BOOST_LOG_SEV(logger(), debug) << "Connecting to TCP redis server on " << n_server << ":" << n_port;

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
	ping(os);

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

		m_status = Status::Connected;
		BOOST_LOG_SEV(logger(), normal) << "Connected to redis";
	});
}



void MRedisTCPConnection::read_response(std::function<void(const RESPonse &)> n_callback) {

	// read one response and evaluate
	asio::async_read_until(m_socket, m_streambuf, "\r\n",
		[this, n_callback](const boost::system::error_code n_errc, const std::size_t) {

			if (handle_error(n_errc, "reading response")) { stop(); return; }

			std::istream is(&m_streambuf);

			RESPonse r = parse_one(is);
			n_callback(r);
	});
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
