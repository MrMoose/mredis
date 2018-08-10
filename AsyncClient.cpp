
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
		, m_server()
		, m_port(0) {

}

AsyncClient::AsyncClient(boost::asio::io_context &n_io_context, const std::string &n_server, const boost::uint16_t n_port /*= 6379*/)
		: m_io_context(n_io_context)
		, m_server(n_server)
		, m_port(n_port) {

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
	m_connection->connect(m_server, m_port);
}

boost::shared_future<bool> AsyncClient::async_connect() {

	if (m_connection) {
		m_connection->stop();
		m_io_context.poll();
		m_connection.reset();
	}

	MOOSE_ASSERT(!m_connection);

	m_connection.reset(new MRedisTCPConnection(*this));

	boost::shared_ptr<boost::promise<bool> > promise(boost::make_shared<boost::promise<bool> >());

	m_connection->async_connect(m_server, m_port, promise);

	return promise->get_future();
}

void AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name,
		const boost::int64_t n_increment_by, Callback &&n_callback /*= Callback()*/) noexcept {

	MOOSE_ASSERT(m_connection);

	m_connection->send(
			[=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); }
			, std::move(n_callback));
}

future_response AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_increment_by /*= 1*/) noexcept {

	// I had planned to move the promise into the handler but this doesn't work as std::function
	// only accepts copyable handlers. Meaning I have to use some kind of pointer type
	// and unique_ptr also doesn't work. Making shared_ptr the only option

	return m_connection->send([=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); })->get_future();		
}

void AsyncClient::hget(const std::string &n_hash_name, const std::string &n_field_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(m_connection);

	m_connection->send(
			[=](std::ostream &n_os) { format_hget(n_os, n_hash_name, n_field_name); }
			, std::move(n_callback));
}

future_response AsyncClient::hget(const std::string &n_hash_name, const std::string &n_field_name) noexcept {

	return m_connection->send([=](std::ostream &n_os) { format_hget(n_os, n_hash_name, n_field_name); })->get_future();
}

void AsyncClient::hset(const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(m_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	m_connection->send(
			[=](std::ostream &n_os) { format_hset(n_os, n_hash_name, n_field_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::hset(const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(m_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	return m_connection->send([=](std::ostream &n_os) { format_hset(n_os, n_hash_name, n_field_name, n_value); })->get_future();
}


void AsyncClient::sadd(const std::string &n_set_name, const std::string &n_value, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(m_connection);

	m_connection->send(
			[=](std::ostream &n_os) { format_sadd(n_os, n_set_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::sadd(const std::string &n_set_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(m_connection);

	return m_connection->send([=](std::ostream &n_os) { format_sadd(n_os, n_set_name, n_value); })->get_future();
}

}
}
