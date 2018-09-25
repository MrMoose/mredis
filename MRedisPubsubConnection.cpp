
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisPubsubConnection.hpp"
#include "AsyncClient.hpp"

#include "tools/Log.hpp"
#include "tools/Assert.hpp"
#include "tools/Random.hpp"

#include <boost/algorithm/string.hpp>

namespace moose {
namespace mredis {

using namespace moose::tools;
namespace asio = boost::asio;
namespace ip = asio::ip;

MRedisPubsubConnection::MRedisPubsubConnection(AsyncClient &n_parent)
		: MRedisConnection(n_parent)
		, m_pending_subscriptions(128)
		, m_subscriptions_pending(0) {

}

MRedisPubsubConnection::~MRedisPubsubConnection() noexcept {

}

void MRedisPubsubConnection::stop() noexcept {

	// We may have pending subscribe requests that are no longer happening
	m_subscriptions_pending.store(0);
	pending_subscription *ps = 0;
	while (m_pending_subscriptions.pop(ps)) {
		if (ps->get<2>()) {
			ps->get<2>()->set_value(false);
			delete ps->get<2>();
		}
		delete ps;
	}

	for (pending_subscription &p : m_pending_confirms) {
		if (ps->get<2>()) {
			ps->get<2>()->set_value(false);
			delete ps->get<2>();
		}
	}
	m_pending_confirms.clear();

	MRedisConnection::stop();
}

// throws std::bad_alloc
boost::future<bool> MRedisPubsubConnection::subscribe(const std::string &n_channel_name, boost::uint64_t *n_id, MessageCallback &&n_callback) {

	MOOSE_ASSERT((n_id));
	BOOST_LOG_SEV(logger(), normal) << "Subscribing to '" << n_channel_name << "'";

	{
		// Let's take care of the handler first and put it in our map
		boost::unique_lock<boost::mutex> slock(m_message_handlers_lock);

		// First we find a unique ID among our handlers
		bool duplicate = false;
		do {
			bool duplicate = false;
			*n_id = tools::urand();
			for (std::map<std::string, SubscriptionMap>::value_type &channel : m_message_handlers) {
				for (SubscriptionMap::value_type &subs : channel.second) {
					if (subs.first == *n_id) {
						duplicate = true;
						break;
					}
				}
			}
		} while (duplicate || (*n_id == 0));

		SubscriptionMap &channel = m_message_handlers[n_channel_name];
		channel[*n_id] = std::move(n_callback);
	}

	// Now send the subscribe message. Since we are still in the caller's thread we must
	// hand things over to io_service now and wait for a future
	boost::promise<bool> *promised_retval = new boost::promise<bool>();

	boost::future<bool> ret = promised_retval->get_future();

	m_pending_subscriptions.push(new pending_subscription{ n_channel_name, 0, promised_retval });
	m_subscriptions_pending++;

	m_socket.get_io_context().post([this] () {

		this->finish_subscriptions();
	});

	if (m_status == Status::Pubsub) {
		// Our connection may be waiting for messages already and not receive any.
		// In order for the io_service to become available, I send a dummy wakeup call 
		// to ourselves. This is channel MREDIS_WAKEUP
		m_parent.publish("MREDIS_WAKEUP", "MOEP!");
	}


	return ret;
}

void MRedisPubsubConnection::unsubscribe(const boost::uint64_t n_id) {

	std::string channel_name;

	{
		// Let's take care of the handler first. An actual unsubscription is only in order if
		// we have no handlers for that channel anymore
		boost::unique_lock<boost::mutex> slock(m_message_handlers_lock);

		// Sadly, the handler could be anywhere in any of our maps	
		for (std::map<std::string, SubscriptionMap>::value_type &channel : m_message_handlers) {
			SubscriptionMap::iterator i = channel.second.find(n_id);
			if (i != channel.second.end()) {
				channel_name = channel.first;
				channel.second.erase(i);  // if channel is empty now, we have set the name to the string
				break;
			} else {
				continue;
			}
		}
	}

	if (channel_name.empty()) {
		BOOST_LOG_SEV(logger(), warning) << "Could not un-subscribe id " << n_id << ", not found";
		return;  // We didn't find our subscription.
	}

	BOOST_LOG_SEV(logger(), normal) << "Unsubscribing from " << channel_name;

	m_pending_subscriptions.push(new pending_subscription{ channel_name, n_id, 0 });
	m_subscriptions_pending++;

	m_parent.io_context().post([this] () {

		this->finish_subscriptions();
	});

	return;
}

void MRedisPubsubConnection::finish_subscriptions() {

	pending_subscription *sub = 0;
	
	try {
		// if the streambuf is still in use so we have to wait for it to become available.
		if (m_buffer_busy) {
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
			BOOST_LOG_SEV(logger(), normal) << "No outstanding subscriptions available";
			// This can happen when we receive a wakeup call someone else sent
			return;
		}

		// we have waited for our buffer to become available. Right now I assume there is 
		// no async operation in progress on it
		MOOSE_ASSERT((!m_buffer_busy));
		m_buffer_busy = true;

		{
			std::ostream os(&m_streambuf);
			if (sub->get<1>() != 0) {
				format_unsubscribe(os, sub->get<0>());
			} else {
				format_subscribe(os, sub->get<0>());
			}
		}

		// send the content of the streambuf to redis
		// We also transfer ownership over the subscription
		asio::async_write(m_socket, m_streambuf,
			[this, sub] (const boost::system::error_code n_errc, const std::size_t) {

				m_buffer_busy = false;

				const std::string channel = sub->get<0>();
				boost::promise<bool> *prm = sub->get<2>();

				m_subscriptions_pending--;

				BOOST_LOG_SEV(logger(), normal) << "Write handler for channel " << channel;

				if (handle_error(n_errc, "sending (un)subscribe command to server")) {	
					if (prm) {
						prm->set_exception(redis_error() << error_message("Could not write subscribe command"));
						delete prm;
					}
					delete sub;
					stop();
					return;
				}

				if (m_status >= Status::ShuttingDown) {
					if (prm) {
						prm->set_exception(redis_error() << error_message("Cannot subscribe. Connection shutting down"));
						delete prm;
					}
					delete sub;
					return;
				}

				// Keep the promise alive while waiting for confirmation.
				// I hand the raw ptr over to this map and wrap it into a unique_ptr 
				// because this map need not be thread safe.
				m_pending_confirms.push_back(*sub);
				delete sub;

				if (!m_pending_confirms.rbegin()->get<1>()) {
					
					// The subscription struct however is now out of scope
					start_pubsub();

					// We should be in Pubsub state now and continuously read messages until 
					// eventually the subscription notification comes in
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
		if (sub->get<2>()) {
			sub->get<2>()->set_exception(redis_error() << error_message("Cannot (un)subscribe"));
			delete sub->get<2>();
		}
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

			// This was a wakeup call to enable us to interrupt message read in order to send subscriptions
			if (boost::algorithm::equals(channel, "MREDIS_WAKEUP")) {
				BOOST_LOG_SEV(logger(), normal) << "Wakeup call received. See to subscriptions";
			} else {
				// Find the appropriate handler(s) and call
				boost::unique_lock<boost::mutex> slock(m_parent.m_message_handlers_lock);

				std::map<std::string, MRedisPubsubConnection::SubscriptionMap>::iterator i = m_parent.m_message_handlers.find(channel);
				if (i == m_parent.m_message_handlers.end()) {
					BOOST_LOG_SEV(logger(), error) << "No subscribed handler for channel '" << channel
							<< "'. This is a bug. We should not be getting this message.";
					return;
				} else {
					try {

						for (MRedisPubsubConnection::SubscriptionMap::value_type &sub : i->second) {
							sub.second(boost::get<std::string>(n_array[2]));
						}

					} catch (const std::exception &sex) {
						BOOST_LOG_SEV(logger(), error) << "Subscribed (no-throw) handler for channel '" << channel
							<< "' threw an exception: " << sex.what();
					} catch (...) {
						BOOST_LOG_SEV(logger(), error) << "Subscribed (no-throw) handler for channel '" << channel
							<< "' threw an unknown exception";
					}
				}
			}

		} else if (boost::algorithm::equals(msgstr, "subscribe")) {

			if (n_array[1].which() != 1) {
				BOOST_LOG_SEV(logger(), error) << "Second subscribe message element is not a string: " << n_array[1].which();
				return;
			}

			// Our subscribe was successful.
			const std::string &channel = boost::get<std::string>(n_array[1]);

			if (boost::algorithm::equals(channel, "MREDIS_WAKEUP")) {
				// internal wakeup call
				return;
			}

			// Find a subscription for that channel in our confirms vector
			std::vector<MRedisPubsubConnection::pending_subscription>::iterator i = std::find_if(
				m_parent.m_pending_confirms.begin(), m_parent.m_pending_confirms.end(),
				[&] (const MRedisPubsubConnection::pending_subscription &p) -> bool {
					return (boost::algorithm::equals(p.get<0>(), channel) && !p.get<1>());
			});

			if (i == m_parent.m_pending_confirms.end()) {
				BOOST_LOG_SEV(logger(), warning) << "Got an unexpected subscribe confirmation for channel '" << channel << "'";
				return;
			}

			// This will notify the original caller that we now are subscribed
			i->get<2>()->set_value(true);
			delete i->get<2>();
			m_parent.m_pending_confirms.erase(i);

		} else if (boost::algorithm::equals(msgstr, "unsubscribe")) {

			if (n_array[1].which() != 1) {
				BOOST_LOG_SEV(logger(), error) << "Second unsubscribe message element is not a string: " << n_array[1].which();
				return;
			}

			const std::string &channel = boost::get<std::string>(n_array[1]);

			// Find a unsubscription for that channel in our confirms vector
			std::vector<MRedisPubsubConnection::pending_subscription>::iterator i = std::find_if(
				m_parent.m_pending_confirms.begin(), m_parent.m_pending_confirms.end(),
				[&] (const MRedisPubsubConnection::pending_subscription &p) -> bool {
					return (boost::algorithm::equals(p.get<0>(), channel) && p.get<1>());
			});


			if (i == m_parent.m_pending_confirms.end()) {
				BOOST_LOG_SEV(logger(), warning) << "Got an unexpected unsubscribe confirmation for channel '" << channel << "'";
				// This is not so bad here. We may be unsubscribing from everything and get confirmations anyway
			} else {
				const boost::uint64_t id = i->get<1>();

				m_parent.m_pending_confirms.erase(i);
				BOOST_LOG_SEV(logger(), normal) << "Unsubscribed from channel '" << channel;

				// Finally remove our handler
				boost::unique_lock<boost::mutex> slock(m_parent.m_message_handlers_lock);			
				m_parent.m_message_handlers[channel].erase(id);
				if (m_parent.m_message_handlers[channel].empty()) {
					m_parent.m_message_handlers.erase(channel);
				}

				if (m_parent.m_message_handlers.empty()) {
					BOOST_LOG_SEV(logger(), normal) << "Last subscription removed, leaving pubsub mode";
					m_parent.m_status = MRedisConnection::Status::Pushing;
				}
			}
		} else {
			BOOST_LOG_SEV(logger(), error) << "Unknown message type: " << n_array[0].which();
			return;
		}
	}

	MRedisPubsubConnection  &m_parent; //!< a reference to the connection that spawned this
};

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
				boost::apply_visitor(MessageVisitor(*this), std::move(r));
			} else {
				break;
			}
		}

		// We may have been woken up by a message, only to be able to see if we 
		// got outstanding subscriptions
		if ((m_subscriptions_pending.load() > 0) && (m_streambuf.size() == 0)) {
			m_buffer_busy = false;
			m_socket.get_io_context().post([this] () {
				this->finish_subscriptions();
			});
			return;
		}

		// handle_message may have caused us to leave pubsub mode by removing the last subscription
		if (m_status != Status::Pubsub) {
			m_buffer_busy = false;
			return;
		}

		// Otherwise read more from the socket
	//	BOOST_LOG_SEV(logger(), debug) << "Reading more messages";

		// read one messages and evaluate
		asio::async_read(m_socket, m_streambuf, asio::transfer_at_least(1),
			[this] (const boost::system::error_code n_errc, const std::size_t) {

				m_buffer_busy = false;

				if (handle_error(n_errc, "reading message")) {
					stop();
					return;
				}

				if (m_status >= Status::ShuttingDown) {
					BOOST_LOG_SEV(logger(), normal) << "Shutting down, not reading any more messages";
					return;
				}
				
				read_message();

// 				if (m_status != MRedisConnection::Status::Pubsub) {
// 					read_message();
// 				} else {
// 					// this is going to parse
// 					if (m_subscriptions_pending.load() > 0) {
// 						this->finish_subscriptions();
// 					} else {
// 						read_message();
// 					}
// 				}
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

}
}
