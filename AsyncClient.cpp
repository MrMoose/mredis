
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "AsyncClient.hpp"
#include "MRedisTCPConnection.hpp"

#include "tools/Assert.hpp"

namespace moose {
namespace mredis {

AsyncClient::AsyncClient(boost::asio::io_context &n_io_context)
		: m_io_context(n_io_context)
		, m_server() {

}

AsyncClient::AsyncClient(boost::asio::io_context &n_io_context, const std::string &n_server, const boost::uint16_t n_port /*= 6379*/)
		: m_io_context(n_io_context)
		, m_server(n_server) {

	MOOSE_ASSERT((!n_server.empty()));

}

AsyncClient::~AsyncClient() noexcept {

	if (m_connection) {
		m_connection->stop();
		m_io_context.poll();
		m_connection.reset();
	}
}

void AsyncClient::connect() {

	if (m_connection) {
		m_connection->stop();
		m_io_context.poll();
		m_connection.reset();
	}

	MOOSE_ASSERT(!m_connection);

	m_connection.reset(new MRedisTCPConnection(*this));
	m_connection->connect(m_server);
}

void AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name,
		const boost::int64_t n_increment_by, Callback &&n_callback /*= Callback()*/) noexcept {

	MOOSE_ASSERT(m_connection);

	m_connection->send_command(
			[&](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); }
	, std::move(n_callback));

}

future_response AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_increment_by /*= 1*/) noexcept {

	// I had planned to move the promise into the handler but this doesn't work as std::function
	// only accepts copyable handlers. Meaning I have to use some kind of pointer type
	// and unique_ptr also doesn't work. Making shared_ptr the only option
	promised_response_ptr promise(boost::make_shared<promised_response>());

	m_connection->send_command(
		[=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); },
		[promise](const RESPonse &n_response) {

			if (n_response.which() == 0) {
				promise->set_exception(boost::get<redis_error>(n_response));
			} else {
				promise->set_value(n_response);
			}
	});

	return promise->get_future();
}

}
}
