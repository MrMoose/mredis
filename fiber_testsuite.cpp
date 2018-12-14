
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "mredis/AsyncClient.hpp"

#include "tools/Log.hpp"
#include "tools/Random.hpp"

#include <boost/config.hpp>
#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/fiber/all.hpp>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <iostream>
#include <cstdlib>

using namespace moose::mredis;
using namespace moose::tools;

std::string server_ip_string;
short port = 6379;

/* This tries to be a fibered stress test for mredis.

   Aim is to spot race conditions and problems by having a scenario 
   with multiple fibers using a single asyncclient instance 

 */


struct SetterFiber {

	public:
		SetterFiber(AsyncClient &n_redis, const unsigned int n_max)
				: m_error(false)
				, m_stopped(false)
				, m_max_value(n_max)
				, m_current_value(0)
				, m_redis(n_redis) {
		}

		void stop() noexcept {

			m_stopped = true;
		}

		void operator()() noexcept {
			try {
				while (!m_stopped && (m_current_value < m_max_value)) {

					// I want to wait for incr's response even though I don't need it, to avoid 
					// just brute force filling the Q without much sense
					std::unique_ptr<boost::fibers::promise<boost::int64_t> > prm(new boost::fibers::promise<boost::int64_t>);
					boost::fibers::promise<boost::int64_t> *stolen_ptr = prm.get();

					m_redis.incr("setter_test_1", [this, stolen_ptr](const RedisMessage &msg) {

						if (is_error(msg)) {
							std::cerr << "Error setting value: " << std::endl;
							std::cerr << boost::diagnostic_information(boost::get<redis_error>(msg)) << std::endl;
							this->m_error = true;
							redis_error ex = boost::get<redis_error>(msg);
							stolen_ptr->set_exception(std::make_exception_ptr(ex));
						} else {
							stolen_ptr->set_value(boost::get<boost::int64_t>(msg));
						}
					});

					boost::fibers::future<boost::int64_t> fv = prm->get_future();

					// Now I think this wait_for would imply a yield... Meaning that other fiber will take over while this one waits
					if (fv.wait_for(std::chrono::seconds(3)) == boost::fibers::future_status::timeout) {
						std::cerr << "Timeout setting value" << std::endl;
						m_error = true;
					}

					// INCR returns value after increment
					if (fv.get() != ++m_current_value) {
						std::cerr << "Value not incremented correctly" << std::endl;
						m_error = true;
					}
				}
			} catch (const moose_error &merr) {
				std::cerr << "Exception caught by SetterFiber: " << boost::diagnostic_information(merr) << std::endl;
				m_error = true;
			}
		}
		
		bool               m_error;

	private:
		bool               m_stopped;
		const unsigned int m_max_value;
		unsigned int       m_current_value;
		AsyncClient       &m_redis;
};

struct GetterFiber {

	public:
		GetterFiber(AsyncClient &n_redis)
				: m_error(false)
				, m_stopped(false)
				, m_redis(n_redis) {
		}

		void stop() noexcept {

			m_stopped = true;
		}

		void operator()() noexcept {
			try {

				boost::this_fiber::sleep_for(std::chrono::milliseconds(10));

				boost::int64_t val = 0;

				while (!m_stopped) {

					std::unique_ptr<boost::fibers::promise<boost::int64_t> > prm(new boost::fibers::promise<boost::int64_t>);
					boost::fibers::promise<boost::int64_t> *stolen_ptr = prm.get();

					const boost::chrono::high_resolution_clock::time_point start = boost::chrono::high_resolution_clock::now();

					m_redis.get("setter_test_1", [this, stolen_ptr](const RedisMessage &msg) {
					
						if (is_error(msg)) {
							std::cerr << "Error getting value: " << std::endl;
							std::cerr << boost::diagnostic_information(boost::get<redis_error>(msg)) << std::endl;
							this->m_error = true;
							redis_error ex = boost::get<redis_error>(msg);
							stolen_ptr->set_exception(std::make_exception_ptr(ex));
						} else if (is_null(msg)) {
							std::cout << "Null response, value not set yet" << std::endl;
							stolen_ptr->set_value(0);
						} else if (is_string(msg)) {
							stolen_ptr->set_value(boost::lexical_cast<boost::int64_t>(boost::get<std::string>(msg)));
						} else {
							stolen_ptr->set_value(boost::get<boost::int64_t>(msg));
						}		
					});

					boost::fibers::future<boost::int64_t> fv = prm->get_future();

					// Now I think this wait_for would imply a yield... Meaning that other fiber will take over while this one waits
					if (fv.wait_for(std::chrono::seconds(3)) == boost::fibers::future_status::timeout) {
						std::cerr << "Timeout getting value" << std::endl;
						m_error = true;
						continue;
					} 

					// Get the value it returned. This should be available now
					if (!fv.valid()) {
						std::cerr << "Future invalid after wait" << std::endl;
						m_error = true;
						continue;
					}

					const boost::int64_t current = fv.get();
					const boost::chrono::high_resolution_clock::time_point finish = boost::chrono::high_resolution_clock::now();

					if (current < val) {
						std::cerr << "Semantic error. Value decreased" << std::endl;
						m_error = true;
					} else {
						// Only randomly output the value so we see things are happening but don't 
						// botch down the slow windows console
						if (current % 500 == 0) {
							std::cout << "got after " << (finish - start) << " -> " << current << std::endl;
						}
						val = current;
					}
				}
			} catch (const moose_error &merr) {
				std::cerr << "Exception caught by GetterFiber: " << boost::diagnostic_information(merr) << std::endl;
				m_error = true;
			}
		}

		bool           m_error;

	private:
		bool           m_stopped;
		AsyncClient   &m_redis;
};

void testcase_1_get_and_set() {

	std::cout << "Running Testcase 1 - Getter and Setter in fibers..." << std::endl;

	AsyncClient client(server_ip_string);
	client.connect();
	
	// Clean slate
	client.del("setter_test_1");

	// continuously increase a value in this fiber
	SetterFiber setter(client, 10000);
	boost::fibers::fiber sf(std::ref(setter));

	// get the value in this fiber
	GetterFiber getter(client);
	boost::fibers::fiber gf(std::ref(getter));

	// This would allow the scheduler to start running the fibers. I guess
//	boost::this_fiber::yield();

	// Wait for the setter to increase 10000 times
	std::cout << "Joining setter fiber" << std::endl;
	sf.join();

	// Tell the getter to stop
	getter.stop();

	// Wait for it to stop
	std::cout << "Joining getter fiber" << std::endl;
	gf.join();

	// Cleanup
	std::cout << "Cleanup" << std::endl;
	client.del("setter_test_1");

	if (getter.m_error) {
		std::cerr << "Getter yielded an error" << std::endl;
	}

	if (setter.m_error) {
		std::cerr << "Setter yielded an error" << std::endl;
	}

	boost::this_fiber::sleep_for(std::chrono::seconds(1));
}


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
		("port,p", po::value<short>()->default_value(6379), "use server port")
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

		server_ip_string = vm["server"].as<std::string>();
		port = vm["port"].as<short>();

		boost::fibers::use_scheduling_algorithm< boost::fibers::algo::round_robin >();

		testcase_1_get_and_set();

		std::cout << "All test cases done" << std::endl;

		return EXIT_SUCCESS;

	} catch (const std::exception &sex) {
		std::cerr << "Unexpected exception reached main: " << sex.what() << std::endl;
	} catch (...) {
		std::cerr << "Unhandled error reached main function. Aborting" << std::endl;
	}

	return EXIT_FAILURE;
}
