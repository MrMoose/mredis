
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
		m_connection.reset();
	}
}

void AsyncClient::connect() {

	if (m_connection) {
		m_connection->stop();
		m_connection.reset();
	}

	MOOSE_ASSERT(!m_connection);

	m_connection.reset(new MRedisTCPConnection(*this));
	m_connection->connect(m_server);
}

}
}
