
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisConnection.hpp"

#include <boost/asio.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/future.hpp>
#include <boost/lockfree/queue.hpp>

#include <string>
#include <map>

namespace moose {
namespace mredis {

class AsyncClient;

class MRedisPubsubConnection : public MRedisConnection {

	public:
		MRedisPubsubConnection(AsyncClient &n_parent);
		MRedisPubsubConnection(const MRedisPubsubConnection &) = delete;
		virtual ~MRedisPubsubConnection() noexcept;
		MRedisPubsubConnection &operator=(const MRedisPubsubConnection &) = delete;
	
		virtual void stop() noexcept;
		
		//! schedule subscription. Promise will be set true when done
		//! @throw exception state may be set on future
		boost::future<bool> subscribe(const std::string &n_channel_name, MessageCallback &&n_callback);

		//! schedule unsubscribe. Promise will be set true when done
		//! @throw exception state may be set on future
		boost::future<bool> unsubscribe(const std::string &n_channel_name);

	private:
		friend class MessageVisitor;

		void finish_subscriptions();

		//! enter pubsub mode by starting to poll for messages
		void start_pubsub();

		//! while in pubsub mode, we are permanently sucking for messages
		void read_message();

		//! when a message came in, analyze and treat by consuming
		void handle_message(const RESPonse &&n_message) noexcept;

		//! For pubsub mode, instead of having one-shot handlers,
		//! I keep them until unsubscribe

		std::map<std::string, MessageCallback>                         m_message_handlers;
		boost::mutex                                                   m_message_handlers_lock;

		//! A list of channel names we still have to subscribe to and promises that shall be broken
		typedef boost::tuple<std::string, boost::promise<bool> *> pending_subscription;
		
		//! requests for subscriptions in here
		boost::lockfree::queue<pending_subscription *>                 m_pending_subscriptions;
	
		//! this signifies that subscriptions are pending
		boost::atomic<unsigned int>                                    m_subscriptions_pending;

		//! same data type goes for unsubscribes too
		boost::lockfree::queue<pending_subscription *>                 m_pending_unsubscribes;

		//! this signifies that unsubscribes are pending
		boost::atomic<unsigned int>                                    m_unsubscribes_pending;

		//! after subscribe request is sent, we keep the promised response until confirm
		//! this doesn't need to be locked or lockfree as access should only occur from io_srv threads
		typedef std::map<std::string, boost::promise<bool> *> confirmation_map;
		confirmation_map                                               m_pending_subscribe_confirms;
		confirmation_map                                               m_pending_unsubscribe_confirms;
};

}
}
