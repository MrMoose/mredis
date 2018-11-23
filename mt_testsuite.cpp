
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "mredis/AsyncClient.hpp"

#include "tools/Log.hpp"
#include "tools/Random.hpp"

#include <boost/config.hpp>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <iostream>
#include <cstdlib>

#define EASY_NUM_SUBSCRIBERS 1


using namespace moose::mredis;
using namespace moose::tools;

/* This tries to be a multithreaded stress test for mredis.

   Aim is to spot race conditions and problems by having a scenario 
   with multiple threads using a single asyncclient instance with 
   some being subsub and some pushing

 */

//! subscribe to one value, read integers and assume they will increase gradually
//! occasionally re-subscribe
class EasySubscriberThread {

	public:
		EasySubscriberThread(AsyncClient &n_redis)
				: m_error(false)
				, m_stopped(false)
				, m_redis(n_redis) {
		}

		void stop() noexcept {
			
			m_stopped.store(true);
		}

		void operator()() noexcept {

			try {
				boost::this_thread::sleep_for(boost::chrono::milliseconds(10));

				while (!m_stopped) {

					m_subscription = m_redis.subscribe("easy_int_test", [this] (const std::string &n_message) {

						boost::int64_t current = boost::lexical_cast<boost::int64_t>(n_message);

						if (current >= m_last) {
							m_last = current;
						} else {
							m_error.store(true);
						}
					});

					// sleep for a random number of seconds, while subscriptions lasts
					boost::this_thread::sleep_for(boost::chrono::seconds(urand(5) + 1));

					m_redis.unsubscribe(m_subscription);
				}
			} catch (const moose_error &merr) {
				std::cerr << "Exception caught by EasySubscriberThread: " << boost::diagnostic_information(merr) << std::endl;
				m_error.store(true);
			}
		}

		boost::atomic<bool>  m_error;          //!< once any error indication is met, this turns true
		boost::int64_t       m_last = 0;

	private:
		boost::uint64_t      m_subscription = 0;
		boost::atomic<bool>  m_stopped;		
		AsyncClient         &m_redis;
};

//! increase a value and publish
class EasyPublisherThread {

	public:
		EasyPublisherThread(AsyncClient &n_redis)
				: m_error(false)
				, m_stopped(false)
				, m_redis(n_redis) {
		}

		void stop() noexcept {

			m_stopped.store(true);
		}

		void operator()() noexcept {

			try {
				boost::int64_t       current = 0;

				boost::this_thread::sleep_for(boost::chrono::milliseconds(10));
				
				while (!m_stopped) {

					future_response r = m_redis.publish("easy_int_test", boost::lexical_cast<std::string>(current));
					current++;

					// Sometimes I wait for the response, sometimes not
					if (urand(1) == 1) {
						// wait for the response
						RedisMessage res = r.get();

						if (!is_int(res)) {
							m_error.store(true);
							std::cerr << "Response to publish is not an int: " << res.which() << std::endl;
							return;
						}

						boost::int64_t i = boost::get<boost::int64_t>(res);

						if (i < 0 || i > EASY_NUM_SUBSCRIBERS) {
							m_error.store(true);
							std::cerr << "Published message claims to have been received by: " << i << std::endl;
							return;
						}
					} else {
						boost::this_thread::sleep_for(boost::chrono::milliseconds(urand(5)));
					}

				}
			} catch (const moose_error &merr) {
				std::cerr << "Exception caught by EasyPublisherThread: " << boost::diagnostic_information(merr) << std::endl;
				m_error.store(true);
			}
		}

		boost::atomic<bool>  m_error;    //!< once any error indication is met, this turns true

	private:
		boost::atomic<bool>  m_stopped;
		AsyncClient         &m_redis;
};



int main(int argc, char **argv) {
	
#ifndef _WIN32
	// core dumps may be disallowed by parent of this process; change that
	struct rlimit core_limits;
	core_limits.rlim_cur = core_limits.rlim_max = RLIM_INFINITY;
	setrlimit(RLIMIT_CORE, &core_limits);
#endif

	moose::tools::init_logging();

	namespace po = boost::program_options;

	po::options_description desc("redis test options");
	desc.add_options()
		("help,h", "Print this help message")
// 		("port,p", po::value<short>()->default_value(7777), "use server port")
		("server,s", po::value<std::string>()->default_value("127.0.0.1"), "give redis server ip");

	try {
		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if (vm.count("help")) {
			std::cout << "Behold your options!\n";
			std::cout << desc << std::endl;
			return EXIT_SUCCESS;
		}

// 		if (!(vm.count("port") && vm.count("server"))) {
// 			std::cerr << "Insufficient parameters" << std::endl;
// 			std::cout << desc << std::endl;
// 			return EXIT_FAILURE;
// 		}
// 
		const std::string server_ip_string = vm["server"].as<std::string>();
// 		const short port = vm["port"].as<short>();

		// Testcase 1 - Easy
		{
			std::cout << "Running Testcase 1 - Easy mode..." << std::endl;

			AsyncClient client(server_ip_string);
		//	client.connect();

			boost::shared_future<bool> ret = client.async_connect();
			const bool r = ret.get();
			if (!r) {
				std::cerr << "Cannot connect" << std::endl;
				return EXIT_FAILURE;
			};

			// Let's start simple by having one thread that publishes an increasing number and three that listen 
			boost::thread_group testers;

			EasyPublisherThread  easy_publisher_1(client);
			EasySubscriberThread easy_subscriber_1(client);
			EasySubscriberThread easy_subscriber_2(client);
// 			EasySubscriberThread easy_subscriber_3(client);

			testers.create_thread(boost::ref(easy_publisher_1));
			testers.create_thread(boost::ref(easy_subscriber_1));
			testers.create_thread(boost::ref(easy_subscriber_2));
// 			testers.create_thread(boost::ref(easy_subscriber_3));

			// Let it run for 30 seconds
			boost::this_thread::sleep_for(boost::chrono::seconds(10));

			// Shut it down
			std::cout << "10 seconds running, shutting down..." << std::endl;

			easy_publisher_1.stop();
			if (easy_publisher_1.m_error) {
				std::cerr << "Error condition in publisher 1" << std::endl;
			}

			easy_subscriber_1.stop();
			if (easy_subscriber_1.m_error) {
				std::cerr << "Error condition in subscriber 1" << std::endl;
			}

			easy_subscriber_2.stop();
			if (easy_subscriber_2.m_error) {
				std::cerr << "Error condition in subscriber 2" << std::endl;
			}

			if (easy_subscriber_1.m_last != easy_subscriber_2.m_last) {
				std::cerr << "All subscribers should have the same end result" << std::endl;
			}

			std::cout << "Subscriber 1 at " << easy_subscriber_1.m_last << std::endl;

// 			easy_subscriber_3.stop();
			testers.join_all();

			std::cout << "Testcase 1 finished. Shutting down..." << std::endl;
		}

		std::cout << "done" << std::endl;

		return EXIT_SUCCESS;

	} catch (const std::exception &sex) {
		std::cerr << "Unexpected exception reached main: " << sex.what() << std::endl;
	} catch (...) {
		std::cerr << "Unhandled error reached main function. Aborting" << std::endl;
	}

	return EXIT_FAILURE;
}
