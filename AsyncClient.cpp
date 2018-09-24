
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "AsyncClient.hpp"
#include "MRedisConnection.hpp"
#include "MRedisPubsubConnection.hpp"
#include "MRedisError.hpp"

#include "tools/Assert.hpp"
#include "tools/Error.hpp"
#include "tools/Log.hpp"

namespace moose {
namespace mredis {

using namespace moose::tools;

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

	if (m_main_connection) {
		m_main_connection->stop();
		m_io_context.poll();
		m_main_connection.reset();
	}

	if (m_pubsub_connection) {
		m_pubsub_connection->stop();
		m_io_context.poll();
		m_pubsub_connection.reset();
	}
}

void AsyncClient::connect() {

	if (m_main_connection) {
		m_main_connection->stop();
		m_io_context.poll();
		m_main_connection.reset();
	}

	if (m_pubsub_connection) {
		m_pubsub_connection->stop();
		m_io_context.poll();
		m_pubsub_connection.reset();
	}

	MOOSE_ASSERT(!m_main_connection);
	MOOSE_ASSERT(!m_pubsub_connection);

	m_main_connection.reset(new MRedisConnection(*this));
	m_main_connection->connect(m_server, m_port);

	m_pubsub_connection.reset(new MRedisPubsubConnection(*this));
	m_pubsub_connection->connect(m_server, m_port);
}

boost::shared_future<bool> AsyncClient::async_connect() {

	if (m_main_connection) {
		m_main_connection->stop();
		m_io_context.poll();
		m_main_connection.reset();
	}

	if (m_pubsub_connection) {
		m_pubsub_connection->stop();
		m_io_context.poll();
		m_pubsub_connection.reset();
	}

	MOOSE_ASSERT(!m_main_connection);
	MOOSE_ASSERT(!m_pubsub_connection);

	m_main_connection.reset(new MRedisConnection(*this));
	boost::shared_ptr<boost::promise<bool> > promise(boost::make_shared<boost::promise<bool> >());
	m_main_connection->async_connect(m_server, m_port, promise);
	
	m_pubsub_connection.reset(new MRedisPubsubConnection(*this));
	boost::shared_ptr<boost::promise<bool> > promise1(boost::make_shared<boost::promise<bool> >());
	m_main_connection->async_connect(m_server, m_port, promise);

	return promise->get_future();
}

void AsyncClient::get(const std::string &n_key, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	m_main_connection->send(
			[=](std::ostream &n_os) { format_get(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::get(const std::string &n_key) noexcept {

	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return m_main_connection->send([=](std::ostream &n_os) { format_get(n_os, n_key); })->get_future();
}

void AsyncClient::set(const std::string &n_key, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	m_main_connection->send(
			[=](std::ostream &n_os) { format_set(n_os, n_key, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::set(const std::string &n_key, const std::string &n_value) noexcept {

	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return m_main_connection->send([=](std::ostream &n_os) { format_set(n_os, n_key, n_value); })->get_future();
}

void AsyncClient::incr(const std::string &n_key, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	m_main_connection->send(
			[=](std::ostream &n_os) { format_incr(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::incr(const std::string &n_key) noexcept {

	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return m_main_connection->send([=](std::ostream &n_os) { format_incr(n_os, n_key); })->get_future();
}

void AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name,
		const boost::int64_t n_increment_by, Callback &&n_callback /*= Callback()*/) noexcept {

	MOOSE_ASSERT(m_main_connection);

	m_main_connection->send(
			[=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); }
			, std::move(n_callback));
}

future_response AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_increment_by /*= 1*/) noexcept {

	// I had planned to move the promise into the handler but this doesn't work as std::function
	// only accepts copyable handlers. Meaning I have to use some kind of pointer type
	// and unique_ptr also doesn't work. Making shared_ptr the only option

	return m_main_connection->send([=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); })->get_future();		
}

void AsyncClient::hget(const std::string &n_hash_name, const std::string &n_field_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(m_main_connection);

	m_main_connection->send(
			[=](std::ostream &n_os) { format_hget(n_os, n_hash_name, n_field_name); }
			, std::move(n_callback));
}

future_response AsyncClient::hget(const std::string &n_hash_name, const std::string &n_field_name) noexcept {

	return m_main_connection->send([=](std::ostream &n_os) { format_hget(n_os, n_hash_name, n_field_name); })->get_future();
}

void AsyncClient::hset(const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	m_main_connection->send(
			[=](std::ostream &n_os) { format_hset(n_os, n_hash_name, n_field_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::hset(const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	return m_main_connection->send([=](std::ostream &n_os) { format_hset(n_os, n_hash_name, n_field_name, n_value); })->get_future();
}

void AsyncClient::sadd(const std::string &n_set_name, const std::string &n_value, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(m_main_connection);

	m_main_connection->send(
			[=](std::ostream &n_os) { format_sadd(n_os, n_set_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::sadd(const std::string &n_set_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(m_main_connection);

	return m_main_connection->send([=](std::ostream &n_os) { format_sadd(n_os, n_set_name, n_value); })->get_future();
}


boost::uint64_t AsyncClient::subscribe(const std::string &n_channel_name, MessageCallback &&n_callback) {

	MOOSE_ASSERT(m_pubsub_connection);

	boost::uint64_t id = 0;

	// This will go async, so I have to wait for the return
	boost::future<bool> retval = m_pubsub_connection->subscribe(n_channel_name, &id, std::move(n_callback));

	// This may throw. I'll pass the exception along
	const bool ret = retval.get();

	if (!ret) {
		BOOST_THROW_EXCEPTION(redis_error()
				<< error_message("Could not subscribe, please check logs for reason")
				<< error_argument(n_channel_name));
	} else {
		BOOST_LOG_SEV(logger(), normal) << "Subscribed to channel '" << n_channel_name << "' with id " << id;
		return id;
	}
}

void AsyncClient::unsubscribe(const boost::uint64_t n_subscription) noexcept {

	MOOSE_ASSERT(m_pubsub_connection);

	try {
		m_pubsub_connection->unsubscribe(n_subscription);

	} catch (const tools::moose_error &merr) {
		BOOST_LOG_SEV(logger(), warning) << "Exception unsubscribing '" << n_subscription << "': "
			<< boost::diagnostic_information(merr);
	} catch (const std::exception &sex) {
		BOOST_LOG_SEV(logger(), warning) << "Unexpected exception unsubscribing '" << n_subscription << "': " << sex.what();
	} catch (...) {
		BOOST_LOG_SEV(logger(), warning) << "Unknown exception unsubscribing '" << n_subscription << "'";
	};
}

future_response AsyncClient::publish(const std::string &n_channel_name, const std::string &n_message) noexcept {
	
	MOOSE_ASSERT(m_main_connection);

	return m_main_connection->send([=] (std::ostream &n_os) { format_publish(n_os, n_channel_name, n_message); })->get_future();
}

}
}
