
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisConnection.hpp"

#include <boost/asio.hpp>
#include <boost/thread/future.hpp>
#include <boost/lockfree/queue.hpp>

#include <string>
#include <map>
#include <atomic>

namespace moose {
namespace mredis {

class AsyncClient;

class MRedisPubsubConnection : public MRedisConnection {

	public:
		MRedisPubsubConnection(AsyncClient &n_parent);
		MRedisPubsubConnection(const MRedisPubsubConnection &) = delete;
		virtual ~MRedisPubsubConnection() noexcept;
		MRedisPubsubConnection &operator=(const MRedisPubsubConnection &) = delete;
	
		void stop() noexcept override;
		
		//! schedule subscription. Promise will be set true when done
		//! @param n_id will be set when subscription returned true
		//! @throw exception state may be set on future
		boost::unique_future<bool> subscribe(const std::string &n_channel_name, boost::uint64_t *n_id, MessageCallback &&n_callback);

		//! schedule unsubscribe.
		//! @throw exception state may be set on future
		void unsubscribe(const boost::uint64_t n_id);

	private:
		friend class MessageVisitor;

		void finish_subscriptions();

		//! enter pubsub mode by starting to poll for messages
		void start_pubsub();

		//! while in pubsub mode, we are permanently sucking for messages
		void read_message();

		//! For pubsub mode, instead of having one-shot handlers,
		//! I keep them until unsubscribe

		//! Subscription ID to handler. We can have many handlers for one channel.
		using SubscriptionMap = std::map<boost::uint64_t, MessageCallback>;

		std::map<std::string, SubscriptionMap>      m_message_handlers;
		boost::mutex                                m_message_handlers_lock;

		//! A list of channel names we still have to subscribe to and promises that shall be broken
		using pending_subscription = boost::tuple<
		                                           std::string            // channel name
		                                         , boost::uint64_t        // 0 when subscribe, set to a subscription ID when unsubscribe
		                                         , boost::promise<bool> * // have ownership, respond when ready, only set for subscriptions
		                                         >;
		
		//! requests for subscriptions in here
		boost::lockfree::queue<pending_subscription *>                 m_pending_subscriptions;
	
		//! this signifies that subscriptions are pending
		std::atomic<unsigned int>                                      m_subscriptions_pending;

		//! after they were sent, I move the subscription tuple into this vector to await confirmation
		//! this doesn't need to be locked or lockfree as access should only occur from io_srv threads
		std::vector<pending_subscription>                              m_pending_confirms;
};

}
}
