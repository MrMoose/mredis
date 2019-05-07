
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "AsyncClient.hpp"
#include "MRedisCommands.hpp"
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
	std::shared_ptr<boost::asio::io_context::work> m_work;
	std::unique_ptr<boost::thread>          m_worker_thread;     //!< the only thread that runs our io_context. implicit strand

	std::string                             m_server;            //!< server hostname if TCP
	boost::uint16_t                         m_port;              //!< server port

	std::unique_ptr<MRedisConnection>       m_main_connection;   //!< this connection handles every major command on a pushing line
	std::unique_ptr<MRedisPubsubConnection> m_pubsub_connection; //!< connection specific for pubsub messages
	bool                                    m_connection_restart;//!< when child connections die, they may request reconnect on demand

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

		// Stop will have 
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
	std::shared_ptr<boost::promise<bool> > promise(std::make_shared<boost::promise<bool> >());
	d().m_main_connection->async_connect(d().m_server, d().m_port, promise);
	
	d().m_pubsub_connection.reset(new MRedisPubsubConnection(*this));
	std::shared_ptr<boost::promise<bool> > promise1(std::make_shared<boost::promise<bool> >());
	d().m_pubsub_connection->async_connect(d().m_server, d().m_port, promise1);

	return promise->get_future();
}

void AsyncClient::time(Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_time(n_os); }
			, std::move(n_callback));
}

future_response AsyncClient::time() noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_time(n_os); })->get_future();
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

void AsyncClient::mget(const std::vector<std::string> &n_keys, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_keys.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_mget(n_os, n_keys); }
			, std::move(n_callback));
}

future_response AsyncClient::mget(const std::vector<std::string> &n_keys) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_keys.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_mget(n_os, n_keys); })->get_future();
}

void AsyncClient::set(const std::string &n_key, const std::string &n_value, Callback &&n_callback,
		const Duration &n_expire_time /* = Duration::max() */, const SetCondition n_condition /* = SetCondition::NONE*/) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_set(n_os, n_key, n_value, n_expire_time, n_condition); }
			, std::move(n_callback));
}

future_response AsyncClient::set(const std::string &n_key, const std::string &n_value,
		const Duration &n_expire_time /* = c_invalid_duration */, const SetCondition n_condition /* = SetCondition::NONE*/) noexcept {

	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_set(n_os, n_key, n_value, n_expire_time, n_condition); })->get_future();
}

void AsyncClient::expire(const std::string &n_key, const Duration &n_expire_time, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_expire(n_os, n_key, n_expire_time); }
			, std::move(n_callback));
}

future_response AsyncClient::expire(const std::string &n_key, const Duration &n_expire_time) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	
	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}


	return d().m_main_connection->send([=](std::ostream &n_os) { format_expire(n_os, n_key, n_expire_time); })->get_future();
}

void AsyncClient::del(const std::string &n_key, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}

	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_del(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::del(const std::string &n_key) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}


	return d().m_main_connection->send([=] (std::ostream &n_os) { format_del(n_os, n_key); })->get_future();
}

void AsyncClient::exists(const std::string &n_key, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	
	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}


	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_exists(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::exists(const std::string &n_key) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	
	if (n_key.empty()) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Key cannot be empty"));
	}

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_exists(n_os, n_key); })->get_future();
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

void AsyncClient::decr(const std::string &n_key, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_decr(n_os, n_key); }
			, std::move(n_callback));
}

future_response AsyncClient::decr(const std::string &n_key) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_key.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_decr(n_os, n_key); })->get_future();
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

void AsyncClient::hlen(const std::string &n_hash_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_hlen(n_os, n_hash_name); }
			, std::move(n_callback));
}

future_response AsyncClient::hlen(const std::string &n_hash_name) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_hlen(n_os, n_hash_name); })->get_future();
}

void AsyncClient::hdel(const std::string &n_hash_name, const std::string &n_field_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_hdel(n_os, n_hash_name, n_field_name); }
			, std::move(n_callback));
}

future_response AsyncClient::hdel(const std::string &n_hash_name, const std::string &n_field_name) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());
	MOOSE_ASSERT(!n_field_name.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_hdel(n_os, n_hash_name, n_field_name); })->get_future();
}

void AsyncClient::hgetall(const std::string &n_hash_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_hgetall(n_os, n_hash_name); }
			, std::move(n_callback));
}

future_response AsyncClient::hgetall(const std::string &n_hash_name) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT(!n_hash_name.empty());

	return d().m_main_connection->send([=](std::ostream &n_os) { format_hgetall(n_os, n_hash_name); })->get_future();
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

void AsyncClient::scard(const std::string &n_set_name, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_scard(n_os, n_set_name); }
			, std::move(n_callback));
}

future_response AsyncClient::scard(const std::string &n_set_name) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_scard(n_os, n_set_name); })->get_future();
}

void AsyncClient::srem(const std::string &n_set_name, const std::string &n_value, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_srem(n_os, n_set_name, n_value); }
			, std::move(n_callback));
}

future_response AsyncClient::srem(const std::string &n_set_name, const std::string &n_value) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_srem(n_os, n_set_name, n_value); })->get_future();
}

void AsyncClient::srandmember(const std::string &n_set_name, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_srandmember(n_os, n_set_name); }
			, std::move(n_callback));
}

future_response AsyncClient::srandmember(const std::string &n_set_name) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_srandmember(n_os, n_set_name); })->get_future();
}

void AsyncClient::smembers(const std::string &n_set_name, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_smembers(n_os, n_set_name); }
			, std::move(n_callback));
}

future_response AsyncClient::smembers(const std::string &n_set_name) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_smembers(n_os, n_set_name); })->get_future();
}

void AsyncClient::eval(const std::string &n_script, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_eval(n_os, n_script, std::vector<std::string>(), std::vector<std::string>()); }
			, std::move(n_callback));
}

future_response AsyncClient::eval(const std::string &n_script) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=] (std::ostream &n_os) {
		format_eval(n_os, n_script, std::vector<std::string>(), std::vector<std::string>()); })->get_future();
}

void AsyncClient::eval(const std::string &n_script, const std::vector<std::string> &n_args, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	std::vector<std::string> keys;

	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_eval(n_os, n_script, keys, n_args); }
			, std::move(n_callback));
}

future_response AsyncClient::eval(const std::string &n_script, const std::vector<std::string> &n_args) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	std::vector<std::string> keys;

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_eval(n_os, n_script, keys, n_args); })->get_future();
}

void AsyncClient::eval(const std::string &n_script, const std::vector<std::string> &n_keys,
		const std::vector<std::string> &n_args, Callback &&n_callback) noexcept {
	
	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=] (std::ostream &n_os) { format_eval(n_os, n_script, n_keys, n_args); }
			, std::move(n_callback));
}

future_response AsyncClient::eval(const std::string &n_script, const std::vector<std::string> &n_keys,
		const std::vector<std::string> &n_args) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_eval(n_os, n_script, n_keys, n_args); })->get_future();
}

void AsyncClient::evalsha(const std::string &n_sha, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT_MSG(!n_sha.empty(), "Must not give empty hash into svalsha()");

	d().m_main_connection->send(
	        [=] (std::ostream &n_os) { format_evalsha(n_os, n_sha, std::vector<std::string>(), std::vector<std::string>()); }
	        , std::move(n_callback));
}

future_response AsyncClient::evalsha(const std::string &n_sha) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT_MSG(!n_sha.empty(), "Must not give empty hash into svalsha()");

	return d().m_main_connection->send([=] (std::ostream &n_os) {
		format_evalsha(n_os, n_sha, std::vector<std::string>(), std::vector<std::string>()); })->get_future();
}

void AsyncClient::evalsha(const std::string &n_sha, const std::vector<std::string> &n_args, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT_MSG(!n_sha.empty(), "Must not give empty hash into svalsha()");

	std::vector<std::string> keys;

	d().m_main_connection->send(
	        [=] (std::ostream &n_os) { format_evalsha(n_os, n_sha, keys, n_args); }
	        , std::move(n_callback));
}

future_response AsyncClient::evalsha(const std::string &n_sha, const std::vector<std::string> &n_args) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT_MSG(!n_sha.empty(), "Must not give empty hash into svalsha()");

	std::vector<std::string> keys;

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_evalsha(n_os, n_sha, keys, n_args); })->get_future();
}

void AsyncClient::evalsha(const std::string &n_sha, const std::vector<std::string> &n_keys,
        const std::vector<std::string> &n_args, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT_MSG(!n_sha.empty(), "Must not give empty hash into svalsha()");

	d().m_main_connection->send(
	        [=] (std::ostream &n_os) { format_evalsha(n_os, n_sha, n_keys, n_args); }
	        , std::move(n_callback));
}

future_response AsyncClient::evalsha(const std::string &n_sha, const std::vector<std::string> &n_keys,
        const std::vector<std::string> &n_args) noexcept {

	MOOSE_ASSERT(d().m_main_connection);
	MOOSE_ASSERT_MSG(!n_sha.empty(), "Must not give empty hash into svalsha()");

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_evalsha(n_os, n_sha, n_keys, n_args); })->get_future();
}

void AsyncClient::script_load(const std::string &n_script, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
	        [=] (std::ostream &n_os) { format_script_load(n_os, n_script); }
	        , std::move(n_callback));
}

future_response AsyncClient::script_load(const std::string &n_script) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=] (std::ostream &n_os) { format_script_load(n_os, n_script); })->get_future();
}

boost::uint64_t AsyncClient::subscribe(const std::string &n_channel_name, MessageCallback &&n_callback) {

	MOOSE_ASSERT(d().m_pubsub_connection);

	boost::uint64_t id = 0;

	// This will go async, so I have to wait for the return
	boost::unique_future<bool> retval = d().m_pubsub_connection->subscribe(n_channel_name, &id, std::move(n_callback));

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

void AsyncClient::debug_sleep(const boost::int64_t n_seconds, Callback &&n_callback) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	d().m_main_connection->send(
			[=](std::ostream &n_os) { format_debug_sleep(n_os, n_seconds); }
			, std::move(n_callback));
}

future_response AsyncClient::debug_sleep(const boost::int64_t n_seconds) noexcept {

	MOOSE_ASSERT(d().m_main_connection);

	return d().m_main_connection->send([=](std::ostream &n_os) { format_debug_sleep(n_os, n_seconds); })->get_future();
}

boost::asio::io_context &AsyncClient::io_context() noexcept {

	return d().m_io_context;
}

void AsyncClient::release_connection(MRedisConnection *n_connection) noexcept {

	if (d().m_main_connection.get() == n_connection) {

		BOOST_LOG_SEV(logger(), warning) << "Main server connection notified it is not working anymore";
		d().m_main_connection.reset();

		// I thought about re-connecting right away but decided to do it on demand.
		// Reason is that whatever caused the connection to drop is very likely still the case,
		// whereas when I defer re-connect to a later state the condition may have passed.
		



	} else if (d().m_pubsub_connection.get() == n_connection) {

		std::cout << "releasing pubsub" << std::endl;
		d().m_pubsub_connection.reset();


	} else {
		BOOST_LOG_SEV(logger(), error) << "Unknown client connection tried to de-register. This is an error.";
	}
}

}
}
