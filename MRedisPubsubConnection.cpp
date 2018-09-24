
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisPubsubConnection.hpp"
#include "AsyncClient.hpp"

#include "tools/Log.hpp"
#include "tools/Assert.hpp"

#include <boost/algorithm/string.hpp>

namespace moose {
namespace mredis {

using namespace moose::tools;
namespace asio = boost::asio;
namespace ip = asio::ip;

MRedisPubsubConnection::MRedisPubsubConnection(AsyncClient &n_parent)
		: MRedisConnection(n_parent)
		, m_pending_subscriptions(128)
		, m_subscriptions_pending(0)
		, m_pending_unsubscribes(128)
		, m_unsubscribes_pending(0) {

}

MRedisPubsubConnection::~MRedisPubsubConnection() noexcept {

}

void MRedisPubsubConnection::stop() noexcept {

	// We may have pending subscribe requests that are no longer happening
	m_subscriptions_pending.store(0);
	pending_subscription *ps = 0;
	while (m_pending_subscriptions.pop(ps)) {
		ps->get<1>()->set_value(false);
		delete ps->get<1>();
		delete ps;
	}

	m_unsubscribes_pending.store(0);
	while (m_pending_unsubscribes.pop(ps)) {
		ps->get<1>()->set_value(false);
		delete ps->get<1>();
		delete ps;
	}

	for (confirmation_map::value_type &p : m_pending_subscribe_confirms) {
		p.second->set_value(false);
	}
	m_pending_subscribe_confirms.clear();

	for (confirmation_map::value_type &p : m_pending_unsubscribe_confirms) {
		p.second->set_value(false);
	}
	m_pending_unsubscribe_confirms.clear();

	MRedisConnection::stop();
}

// throws std::bad_alloc
boost::future<bool> MRedisPubsubConnection::subscribe(const std::string &n_channel_name, MessageCallback &&n_callback) {

	BOOST_LOG_SEV(logger(), normal) << "Subscribing to '" << n_channel_name << "'";

	{
		// Let's take care of the handler first and put it in our map
		boost::unique_lock<boost::mutex> slock(m_message_handlers_lock);
		m_message_handlers[n_channel_name] = std::move(n_callback);
	}

	// Now send the subscribe message. Since we are still in the caller's thread we must
	// hand things over to io_service now and wait for a future
	boost::promise<bool> *promised_retval = new boost::promise<bool>();

	boost::future<bool> ret = promised_retval->get_future();

	pending_subscription *tmp = new pending_subscription(n_channel_name, promised_retval);

	m_pending_subscriptions.push(tmp);
	m_subscriptions_pending++;

	m_parent.m_io_context.post([this] () {

		this->finish_subscriptions();
	});

	return ret;
}

boost::future<bool> MRedisPubsubConnection::unsubscribe(const std::string &n_channel_name) {

	BOOST_LOG_SEV(logger(), normal) << "Unsubscribing from '" << n_channel_name << "'";

	// Now send the subscribe message. Since we are still in the caller's thread we must
	// hand things over to io_service now and wait for a future
	boost::promise<bool> *promised_retval = new boost::promise<bool>();

	boost::future<bool> ret = promised_retval->get_future();

	m_pending_unsubscribes.push(new pending_subscription(n_channel_name, promised_retval));
	m_unsubscribes_pending++;

	m_parent.m_io_context.post([this] () {

		this->finish_subscriptions();
	});

	return ret;
}

void MRedisPubsubConnection::finish_subscriptions() {

	pending_subscription *sub = 0;
	bool unsub = false; // I use this function for both subscribe and unsubscribe. This gets true when latter

	try {
		// if the streambuf is still in use so we have to wait for it to become available.
		if (m_buffer_busy) {

			// it is very likely an async operation is listening on the socket waiting 
			// for messages. In order to safely do so, the buffer is marked busy. I would have 
			// to wait for the next message in order to make this happen, which is not sensible.
			// 
			// Right now I see no other way but to shut down the async operation while keeping 
			// the socket alive. 
			//
			if (m_status == Status::Pubsub) {
				boost::system::error_code err;
				m_socket.cancel(err);      // This should immediately cancel the message read
				if (err) {
					BOOST_LOG_SEV(logger(), warning) << "Could not cancel socket operation trying to subscribe/unsubscribe: " << err.message();
				}
			}

			m_send_retry_timer.expires_after(asio::chrono::milliseconds(1));
			m_send_retry_timer.async_wait(
				[this] (const boost::system::error_code &n_err) {

					if (!n_err && (this->m_status == Status::Pushing || this->m_status == Status::Pubsub)) {
						this->finish_subscriptions();
					}
			});
			return;
		}

		const bool subscribe_available = m_pending_subscriptions.pop(sub);
		if (!subscribe_available) {		
			const bool unsubscribe_available = m_pending_unsubscribes.pop(sub);
			if (!unsubscribe_available) {
				BOOST_LOG_SEV(logger(), warning) << "No outstanding subscriptions available";
				// This should not happen as we only always call this after we filled the q
				// or after we waited for the buffer to clear. So I don't go into retry but
				return;
			} else {
				unsub = true;
			}
		}

		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		{
			std::ostream os(&m_streambuf);
			if (unsub) {
				format_unsubscribe(os, sub->get<0>());
			} else {
				format_subscribe(os, sub->get<0>());
			}
		}

		// send the content of the streambuf to redis
		// We also transfer ownership over the subscription
		asio::async_write(m_socket, m_streambuf,
			[this, sub, unsub] (const boost::system::error_code n_errc, const std::size_t) {

				m_buffer_busy = false;

				const std::string channel = sub->get<0>();
				boost::promise<bool> *prm = sub->get<1>();

				if (handle_error(n_errc, "sending (un)subscribe command to server")) {				
					prm->set_exception(redis_error() << error_message("Could not write (un)subscribe command"));
					delete prm;
					delete sub;
					stop();
					return;
				}

				if (m_status >= Status::ShuttingDown) {
					prm->set_exception(redis_error() << error_message("Cannot (un)subscribe. Connection shutting down"));
					delete prm;
					delete sub;
					return;
				}

				// Keep the promise alive while waiting for confirmation.
				// I hand the raw ptr over to this map and wrap it into a unique_ptr 
				// because this map need not be thread safe.
				if (!unsub) {
					m_pending_subscribe_confirms.emplace(std::make_pair(channel, prm));
					m_subscriptions_pending--;
				
					// The subscription struct however is now out of scope
					start_pubsub();

					// We should be in Pubsub state now and continuously read messages until 
					// eventually the subscription notification comes in
				} else {
					m_pending_unsubscribe_confirms.emplace(std::make_pair(channel, prm));
					m_unsubscribes_pending--;

					// We remain in pubsub mode until the last subscription is confirmed gone
				}
		});

		return;

	} catch (const boost::system::system_error &serr) {
		// Very likely we failed to set a retry timer. Doesn't matter. We still wake up as the next command comes in
		BOOST_LOG_SEV(logger(), warning) << "Error sending (un)subscribe command: " << boost::diagnostic_information(serr);
	} catch (const tools::moose_error &merr) {
		// Not expected at all
		BOOST_LOG_SEV(logger(), warning) << "Error sending (un)subscribe command: " << boost::diagnostic_information(merr);
	} catch (const std::exception &sex) {
		// The caller didn't provide callbacks that were nothrow
		BOOST_LOG_SEV(logger(), error) << "Unexpected exception in finish_subscriptions(): " << boost::diagnostic_information(sex);
	}

	if (sub) {	
		sub->get<1>()->set_exception(redis_error() << error_message("Cannot (un)subscribe"));
		delete sub->get<1>();
		delete sub;
		sub = 0;
	}
}

void MRedisPubsubConnection::start_pubsub() {

	// We are now in Pub/sub mode
	if (m_status == Status::Pushing) {
		m_status = Status::Pubsub;
		BOOST_LOG_SEV(logger(), normal) << "Connection entered pub/sub mode";
		read_message();
	} else if (m_status == Status::Pubsub) {
		// we are already in pubsub mode and do nothing
		BOOST_LOG_SEV(logger(), normal) << "Request sent, resuming pubsub";
		read_message();
	} else {
		// We should not be here at all
	}
}

void MRedisPubsubConnection::read_message() {
	
	if (m_status >= Status::ShuttingDown) {
		return;
	}

	try {
		// if the streambuf is in use we cannot push data into it
		// we have to wait for it to become available. We store both handlers until later
		if (m_buffer_busy) {	
			m_receive_retry_timer.expires_after(asio::chrono::milliseconds(1));
			m_receive_retry_timer.async_wait(
				[this] (const boost::system::error_code &n_err) {

					if (!n_err && this->m_status == Status::Pubsub) {
						this->read_message();
					}
			});
		
			return;
		}

		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		// perhaps we already have bytes to read in our streambuf. If so, I parse those first
		while (m_streambuf.size()) {

			std::istream is(&m_streambuf);
			RESPonse r;

			// As long as we can parse messages from our stream, continue to do so.
			bool success = parse_from_stream(is, r);
			if (success) {
				handle_message(std::move(r));
			} else {
				break;
			}
		}

		// handle_message may have caused us to leave pubsub mode by removing the last subscription
		if (m_status != Status::Pubsub) {
			return;
		}

		// Otherwise read more from the socket
		BOOST_LOG_SEV(logger(), debug) << "Reading more messages";

		// read one messages and evaluate
		asio::async_read(m_socket, m_streambuf, asio::transfer_at_least(1),
			[this] (const boost::system::error_code n_errc, const std::size_t) {

				m_buffer_busy = false;

				// This is a special case. We continuously read messages. When we get a new subscription
				// request though we must interrupt this, which causes this handler to abort
				if ((n_errc == boost::asio::error::operation_aborted) &&
						((m_subscriptions_pending.load() > 0) || (m_unsubscribes_pending.load() > 0))) {
					this->finish_subscriptions();
					return;
				}

				if (handle_error(n_errc, "reading message")) {
					stop();
					return;
				}

				if (m_status >= Status::ShuttingDown) {
					BOOST_LOG_SEV(logger(), normal) << "Shutting down, not reading any more messages";
					return;
				}

				if (m_status != MRedisConnection::Status::Pubsub) {
					read_message();
				} else {
					// this is going to parse
					if ((m_subscriptions_pending.load() > 0) || (m_unsubscribes_pending.load() > 0)) {
						this->finish_subscriptions();
					} else {
						read_message();
					}
				}
		});
	} catch (const boost::system::system_error &serr) {
		// Very likely we failed to set a retry timer. Doesn't matter. We still wake up as the next command comes in
		BOOST_LOG_SEV(logger(), warning) << "Error reading message: " << boost::diagnostic_information(serr);
	} catch (const tools::moose_error &merr) {
		// Not expected at all
		BOOST_LOG_SEV(logger(), warning) << "Error reading message: " << boost::diagnostic_information(merr);
	} catch (const std::exception &sex) {
		// The caller didn't provide callbacks that were nothrow
		BOOST_LOG_SEV(logger(), error) << "Unexpected exception in read_response(): " << boost::diagnostic_information(sex);
	}
}

class MessageVisitor : public boost::static_visitor<> {

	public:
		MessageVisitor(MRedisPubsubConnection &n_connection)
				: m_parent(n_connection) {
		}

		void operator()(const redis_error &n_error) const {
		
			BOOST_LOG_SEV(logger(), error) << "Received error message, while expecting an array: " << boost::diagnostic_information(n_error);
		}

		void operator()(const std::string &n_string) const {
	
			BOOST_LOG_SEV(logger(), error) << "Received string message, while expecting an array: " << n_string;
		}

		void operator()(const boost::int64_t &n_integer) const {

			BOOST_LOG_SEV(logger(), error) << "Received integer message, while expecting an array: " << n_integer;
		}

		void operator()(const null_result &n_null) const {
		
			BOOST_LOG_SEV(logger(), error) << "Received null message, while expecting an array";
		}

		void operator()(const std::vector<RESPonse> &n_array) const {
	
			// From the redis specs:
			// A message is a Array reply with three elements.
			// The first element is the kind of message :
			//   subscribe: means that we successfully subscribed to the channel given as the second 
			//              element in the reply. The third argument represents the number of channels
			//              we are currently subscribed to.
			//   unsubscribe: means that we successfully unsubscribed from the channel given as second 
			//                element in the reply. The third argument represents the number of channels
			//                we are currently subscribed to. When the last argument is zero, we are no
			//                longer subscribed to any channel, and the client can issue any kind of 
			//                Redis command as we are outside the Pub / Sub state.
			//   message: it is a message received as result of a PUBLISH command issued by another 
			//            client.The second element is the name of the originating channel, and the
			//            third argument is the actual message payload.
			//
		
			if (n_array.size() != 3) {
				BOOST_LOG_SEV(logger(), error) << "Received array message with wrong number of elements: " << n_array.size();
				return;
			}
	
			// first element must be a string
			if (n_array[0].which() != 1) {
				BOOST_LOG_SEV(logger(), error) << "First message element is not a string: " << n_array[0].which();
				return;
			}
	
			const std::string &msgstr = boost::get<std::string>(n_array[0]);
	
			if (boost::algorithm::equals(msgstr, "message")) {
	
				if (n_array[1].which() != 1) {
					BOOST_LOG_SEV(logger(), error) << "Second message element is not a string: " << n_array[1].which();
					return;
				}

				// We know the message channel now
				const std::string &channel = boost::get<std::string>(n_array[1]);

				// Get the appropriate handler and call it
				boost::unique_lock<boost::mutex> slock(m_parent.m_message_handlers_lock);
				std::map<std::string, MessageCallback>::iterator i = m_parent.m_message_handlers.find(channel);
				if (i == m_parent.m_message_handlers.end()) {
					BOOST_LOG_SEV(logger(), error) << "No subscribed handler for channel '" << channel 
							<< "'. This is a bug. We should not be getting this message.";
					return;
				} else {
					try {
						i->second(boost::get<std::string>(n_array[2]));
					} catch (const std::exception &sex) {
						BOOST_LOG_SEV(logger(), error) << "Subscribed (no-throw) handler for channel '" << channel
								<< "' threw an exception: " << sex.what();
					} catch (...) {
						BOOST_LOG_SEV(logger(), error) << "Subscribed (no-throw) handler for channel '" << channel
								<< "' threw an unknown exception";
					}
				}

			} else if (boost::algorithm::equals(msgstr, "subscribe")) {
				
				if (n_array[1].which() != 1) {
					BOOST_LOG_SEV(logger(), error) << "Second subscribe message element is not a string: " << n_array[1].which();
					return;
				}
	
				// Our subscribe was successful.
				const std::string &channel = boost::get<std::string>(n_array[1]);
	
				MRedisPubsubConnection::confirmation_map::iterator i = m_parent.m_pending_subscribe_confirms.find(channel);

				if (i == m_parent.m_pending_subscribe_confirms.end()) {
					BOOST_LOG_SEV(logger(), warning) << "Got an unexpected subscribe confirmation for channel '" << channel << "'";
					return;
				} 
				
				// This will notify the original caller that we now are subscribed
				i->second->set_value(true);
			
				// We should be subscribed to as many channels now.
				// Let's be harsh during development and assert this shit
				{
					// Let's take care of the handler first and put it in our map
					boost::unique_lock<boost::mutex> slock(m_parent.m_message_handlers_lock);
					MOOSE_ASSERT(boost::get<boost::int64_t>(n_array[2]) == m_parent.m_message_handlers.size());
				}

				delete i->second;
				m_parent.m_pending_subscribe_confirms.erase(i);

		} else if (boost::algorithm::equals(msgstr, "unsubscribe")) {
		
			if (n_array[1].which() != 1) {
				BOOST_LOG_SEV(logger(), error) << "Second unsubscribe message element is not a string: " << n_array[1].which();
				return;
			}

			// Our subscribe was successful.
			const std::string &channel = boost::get<std::string>(n_array[1]);

			MRedisPubsubConnection::confirmation_map::iterator i = m_parent.m_pending_unsubscribe_confirms.find(channel);
			
			if (i == m_parent.m_pending_unsubscribe_confirms.end()) {
				BOOST_LOG_SEV(logger(), warning) << "Got an unexpected unsubscribe confirmation for channel '" << channel << "'";

				// This is not so bad here. We may be unsubscribing from everything and get confirmations anyway
			} else {
				// This will notify the original caller that we now are subscribed
				i->second->set_value(true);
				m_parent.m_pending_unsubscribe_confirms.erase(i);
			}
			
			// Finally remove our handler
			boost::unique_lock<boost::mutex> slock(m_parent.m_message_handlers_lock);
			m_parent.m_message_handlers.erase(channel);

			MOOSE_ASSERT(boost::get<boost::int64_t>(n_array[2]) == m_parent.m_message_handlers.size());

			if (m_parent.m_message_handlers.empty()) {
				BOOST_LOG_SEV(logger(), normal) << "Last subscription removed, leaving pubsub mode";
				m_parent.m_status = MRedisConnection::Status::Pushing;
			}

		} else {
			BOOST_LOG_SEV(logger(), error) << "Unknown message type: " << n_array[0].which();
			return;
		}
	}

	MRedisPubsubConnection  &m_parent; //!< a reference to the connection that spawned this
};

void MRedisPubsubConnection::handle_message(const RESPonse &&n_message) noexcept {

	boost::apply_visitor(MessageVisitor(*this), n_message);
}

}
}
