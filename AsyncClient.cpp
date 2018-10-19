
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

#include <boost/thread.hpp>

namespace moose {
namespace mredis {

using namespace moose::tools;

struct AsyncClientMembers : public moose::tools::Pimplee {

	AsyncClientMembers()
			: m_io_context()
			, m_work()
			, m_worker_thread()
			, m_server()
			, m_port(0) {
	}

	~AsyncClientMembers() noexcept {
		
		// cleanup of io_service in AsyncClient d'tor
	}

	boost::asio::io_context                 m_io_context;        //!< may be owned by this or running outside
	boost::shared_ptr<boost::asio::io_context::work> m_work;
	std::unique_ptr<boost::thread>          m_worker_thread;     //!< the only thread that runs our io_context. implicit strand

	std::string                             m_server;            //!< server hostname if TCP
	boost::uint16_t                         m_port;              //!< server port
	std::unique_ptr<MRedisConnection>       m_main_connection;   //!< this connection handles every major command on a pushing line
	std::unique_ptr<MRedisPubsubConnection> m_pubsub_connection; //!< connection specific for pubsub messages
};


AsyncClient::AsyncClient() : moose::tools::Pimpled<AsyncClientMembers>() {

	d().m_work.reset(new boost::asio::io_context::work(d().m_io_context));
	d().m_worker_thread.reset(new boost::thread([&] () { d().m_io_context.run(); }));
}

AsyncClient::AsyncClient(const std::string &n_server, const boost::uint16_t n_port /*= 6379*/) {

	d().m_server = n_server;
	d().m_port = n_port;

	MOOSE_ASSERT((!d().m_server.empty()));
	
	d().m_work.reset(new boost::asio::io_context::work(d().m_io_context));
	d().m_worker_thread.reset(new boost::thread([&] () { d().m_io_context.run(); }));

	BOOST_LOG_SEV(logger(), normal) << "AsyncClient started";
}

AsyncClient::~AsyncClient() noexcept {

	if (d().m_main_connection) {
		d().m_main_connection->stop();
		d().m_io_context.poll();
		d().m_main_connection.reset();
	}

	if (d().m_pubsub_connection) {
		d().m_pubsub_connection->stop();
		d().m_io_context.poll();
		d().m_pubsub_connection.reset();
	}

	d().m_io_context.stop();

	// wait for 
	if (d().m_worker_thread) {
		d().m_work.reset();
		d().m_worker_thread->join();
		d().m_worker_thread.reset();
	}

	BOOST_LOG_SEV(logger(), normal) << "AsyncClient stopped";
}

void AsyncClient::connect() {

	if (d().m_main_connection) {
		d().m_main_connection->stop();
		d().m_io_context.poll();
		d().m_main_connection.reset();
	}

	if (d().m_pubsub_connection) {
		d().m_pubsub_connection->stop();
		d().m_io_context.poll();
		d().m_pubsub_connection.reset();
	}

	MOOSE_ASSERT(!d().m_main_connection);
	MOOSE_ASSERT(!d().m_pubsub_connection);

	d().m_main_connection.reset(new MRedisConnection(*this));
	d().m_main_connection->connect(d().m_server, d().m_port);

	d().m_pubsub_connection.reset(new MRedisPubsubConnection(*this));
	d().m_pubsub_connection->connect(d().m_server, d().m_port);
}

boost::shared_future<bool> AsyncClient::async_connect() {

	if (d().m_main_connection) {
		d().m_main_connection->stop();
		d().m_io_context.poll();
		d().m_main_connection.reset();
	}

	if (d().m_pubsub_connection) {
		d().m_pubsub_connection->stop();
		d().m_io_context.poll();
		d().m_pubsub_connection.reset();
	}

	MOOSE_ASSERT(!d().m_main_connection);
	MOOSE_ASSERT(!d().m_pubsub_connection);

	d().m_main_connection.reset(new MRedisConnection(*this));
	boost::shared_ptr<boost::promise<bool> > promise(boost::make_shared<boost::promise<bool> >());
	d().m_main_connection->async_connect(d().m_server, d().m_port, promise);
	
	d().m_pubsub_connection.reset(new MRedisPubsubConnection(*this));
	boost::shared_ptr<boost::promise<bool> > promise1(boost::make_shared<boost::promise<bool> >());
	d().m_pubsub_connection->async_connect(d().m_server, d().m_port, promise1);

	return promise->get_future();
}

void AsyncClient::get(const std::string &n_key, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_get(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::get(const std::string &n_key) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_get(n_os, n_key); })->get_future();
}

void AsyncClient::set(const std::string &n_key, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_set(n_os, n_key, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::set(const std::string &n_key, const std::string &n_value) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_set(n_os, n_key, n_value); })->get_future();
}

void AsyncClient::incr(const std::string &n_key, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_incr(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::incr(const std::string &n_key) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_incr(n_os, n_key); })->get_future();
}

void AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name,
		const boost::int64_t n_increment_by, Callback &&n_callback /*= Callback()*/) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); }
			, std::move(n_callback));
}

future_response AsyncClient::hincrby(const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_increment_by /*= 1*/) noexcept {

	// I had planned to move the promise into the handler but this doesn't work as std::function
	// only accepts copyable handlers. Meaning I have to use some kind of pointer type
	// and unique_ptr also doesn't work. Making shared_ptr the only option

	return d().m_main_connection->send([=](std::ostream &n_os) { format_hincrby(n_os, n_hash_name, n_field_name, n_increment_by); })->get_future();
}

void AsyncClient::hget(const std::string &n_hash_name, const std::string &n_field_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_hget(n_os, n_hash_name, n_field_name); }
			, std::move(n_callback));
}

future_response AsyncClient::hget(const std::string &n_hash_name, const std::string &n_field_name) noexcept {

	return d().m_main_connection->send([=](std::ostream &n_os) { format_hget(n_os, n_hash_name, n_field_name); })->get_future();
}

void AsyncClient::hset(const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_hset(n_os, n_hash_name, n_field_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::hset(const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_hset(n_os, n_hash_name, n_field_name, n_value); })->get_future();
}

void AsyncClient::lpush(const std::string &n_list_name, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_list_name.empty());
	MOOSE_ASSERT(!n_value.empty());

	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_lpush(n_os, n_list_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::lpush(const std::string &n_list_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_list_name.empty());
	MOOSE_ASSERT(!n_value.empty());

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_lpush(n_os, n_list_name, n_value); })->get_future();
}

void AsyncClient::rpush(const std::string &n_list_name, const std::string &n_value, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_list_name.empty());
	MOOSE_ASSERT(!n_value.empty());

	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_rpush(n_os, n_list_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::rpush(const std::string &n_list_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_list_name.empty());
	MOOSE_ASSERT(!n_value.empty());

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_rpush(n_os, n_list_name, n_value); })->get_future();
}

void AsyncClient::sadd(const std::string &n_set_name, const std::string &n_value, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_sadd(n_os, n_set_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::sadd(const std::string &n_set_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_sadd(n_os, n_set_name, n_value); })->get_future();
}


boost::uint64_t AsyncClient::subscribe(const std::string &n_channel_name, MessageCallback &&n_callback) {

	MOOSE_ASSERT(d().m_pubsub_connection);

	boost::uint64_t id = 0;

	// This will go async, so I have to wait for the return
	boost::future<bool> retval = d().m_pubsub_connection->subscribe(n_channel_name, &id, std::move(n_callback));

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

	MOOSE_ASSERT(d().m_pubsub_connection);

	try {
		d().m_pubsub_connection->unsubscribe(n_subscription);

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
	
	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_publish(n_os, n_channel_name, n_message); })->get_future();
}

boost::asio::io_context &AsyncClient::io_context() noexcept {

	return d().m_io_context;
}

}
}
