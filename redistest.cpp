
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "mredis/AsyncClient.hpp"
#include "mredis/BlockingRetriever.hpp"

#include "tools/Log.hpp"
#include "tools/Error.hpp"
#include "tools/Random.hpp"

#include <boost/config.hpp>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <boost/program_options.hpp>
#include <boost/fiber/all.hpp>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <iostream>
#include <cstdlib>
#include <string>

using namespace moose::mredis;
using namespace moose::tools;

using FPromisedRedisMessage = boost::fibers::promise< RedisMessage >;
using FFutureRedisMessage = boost::fibers::future< RedisMessage >;

std::string server_ip_string;

void output_int_result(future_response &&n_response) {

	RedisMessage response = n_response.get();

	if (is_int(response)) {
		std::cout << "Response: " << boost::get<boost::int64_t>(response) << std::endl;
	} else {
		std::cerr << "Unexpected response: " << response.which() << std::endl;
	};
}

void expect_some_result(future_response &&n_response) {

	if (n_response.wait_for(boost::chrono::seconds(5)) == boost::future_status::timeout) {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout expecting value"));
	}

	// whatever, just return
	RedisMessage response = n_response.get();
}

void expect_int_result(future_response &&n_response, const boost::int64_t n_expected_value) {

	RedisMessage response = n_response.get();

	if (is_int(response)) {
		if (boost::get<boost::int64_t>(response) != n_expected_value) {
			BOOST_THROW_EXCEPTION(redis_error() << error_message("Unexpected int response")
				<< error_argument(boost::get<boost::int64_t>(response)));
		}
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not an int response"));
	};
}

void expect_string_result(future_response &&n_response) {

	RedisMessage response = n_response.get();

	if (is_string(response)) {
		std::cout << "Got string response: " << boost::get<std::string>(response) << std::endl;
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not a string response"));
	};
}

void expect_string_result(const RedisMessage &n_response, const std::string &n_expected_string) {

	if (is_string(n_response)) {
		if (boost::get<std::string>(n_response) != n_expected_string) {
			BOOST_THROW_EXCEPTION(redis_error() << error_message("Unexpected string response")
				<< error_argument(boost::get<std::string>(n_response)));
		}
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not a string response"));
	};
}

void expect_string_result(FFutureRedisMessage &&n_message, const std::string &n_expected_string) {

	RedisMessage response = n_message.get();
	expect_string_result(response, n_expected_string);
}

void expect_string_result(future_response &&n_response, const std::string &n_expected_string) {

	RedisMessage response = n_response.get();
	expect_string_result(response, n_expected_string);
}

void expect_null_result(future_response &&n_response) {

	RedisMessage response = n_response.get();

	if (is_null(response)) {
		std::cout << "Got expected null response" << std::endl;
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not a null response"));
	};
}

bool test_binary_get() {
	
	// Test getting and setting a binary value with at least one null byte in it.
	const std::string binary_sample("Hello\0 World", 12);

	AsyncClient client(server_ip_string);
	client.connect();
	
	expect_string_result(client.set("redistest:myval:437!:bin_test_key", binary_sample), "OK");

	future_response sr1 = client.get("redistest:myval:437!:bin_test_key");
	RedisMessage br1 = sr1.get();

	// I expect the response to be a string containing the same binary value
	if (!is_string(br1)) {
		std::cerr << "not a string response: " << br1.which() << std::endl;
		return false;
	} else {
		if (binary_sample != boost::get<std::string>(br1)) {
			std::cerr << "Binary set failed: " << boost::get<std::string>(br1) << std::endl;
			return false;
		}
	}

	client.del("redistest:myval:437!:bin_test_key");
	return true;
}

// test the eval command a bit. I'll try not to actually test Lua as this is supposed to 
// be way out of scope. So I'll keep the actual scripts minimal
bool test_lua() {

	try {
		AsyncClient client(server_ip_string);
		client.connect();

		// Very simple set
		expect_string_result(
			client.eval("return redis.call('set', 'redistest:foo', 'bar')"),
			"OK"
		);

		// check for error condition. I want to know if get for a nonexisting value is an error or 0
		expect_null_result(client.eval("return redis.call('get', 'fooo')"));
		expect_null_result(client.eval("return redis.pcall('get', 'fooo')"));
		expect_null_result(client.eval("return tonumber(redis.pcall('get', 'fooo'))"));

		// to be used many times
		std::vector<std::string> keys;
		std::vector<std::string> args;

		// Set with binary arguments
		keys.emplace_back(std::string("redistest:Hel\r\nlo", 17));
		args.emplace_back(std::string("W\0rld", 5));

		expect_string_result(
			client.eval("return redis.call('set', KEYS[1], ARGV[1])", keys, args),
			"OK"
		);
	
		// get the binary string back using regular get and expect to be same
		expect_string_result(client.get(std::string("redistest:Hel\r\nlo", 17)), std::string("W\0rld", 5));
	
		keys.clear();
		args.clear();

		// a little more complex script that increases a number of seats and occupies one if available
		expect_string_result(client.set("redistest:used_seats", "3"), "OK");

		const std::string add_seat( // It appears as if everything is type-less stored as string. 
		                        // In order to make Lua know I intend to treat it as a number, I have
		                        // to explicitly use tonumber()
			"local used_seats = tonumber(redis.call('get', KEYS[1])) "
			"if used_seats < 4 then "
				"redis.call('incr', KEYS[1]) "
				"redis.call('set', KEYS[2], ARGV[1]) "
				"return 'OK' "
			"else "
				"return nil "
			"end"
		);

		keys.emplace_back("redistest:used_seats");
		keys.emplace_back("redistest:seat4");

		args.emplace_back("Moose");

		// This should work once
		expect_string_result(client.eval(add_seat, keys, args), "OK");

		keys[1] = "redistest:seat5";
		args[0] = "PoorBugger";

		// But not again because all seats are used
		expect_null_result(client.eval(add_seat, keys, args));

		// Which means, we only have entry one
		expect_string_result(client.get("redistest:seat4"), "Moose");
		expect_null_result(client.get("redistest:seat5"));

		// cleanup
		expect_some_result(client.del("redistest:used_seats"));
		expect_some_result(client.del("redistest:seat4"));
		expect_some_result(client.del(std::string("redistest:Hel\r\nlo", 17)));
		expect_some_result(client.del("redistest:foo"));

		return true;

	} catch (const moose::mredis::redis_error &merr) {
		std::cerr << "Error testing lua eval(): " << boost::diagnostic_information(merr) << std::endl;
	} catch (...) {
		std::cerr << "Unhandled exception testing lua eval()" << std::endl;
	}

	return false;
}

// Test setting a value with additional parameters
bool test_extended_set_params() {

	try {
		// Test getting and setting a binary value with at least one null byte in it.
		const std::string sample("Hello World!");

		AsyncClient client(server_ip_string);
		client.connect();

		// Delete possibly existing test value
		expect_some_result(client.del("redistest:no_exp"));

		// First try to set value without XX should fail
		expect_null_result(client.set("redistest:no_exp", sample, c_invalid_duration, SetCondition::XX));
	
		// Now set the value with NX should succeed
		expect_string_result(client.set("redistest:no_exp", sample, c_invalid_duration, SetCondition::NX), "OK");

		// Check the value
		expect_string_result(client.get("redistest:no_exp"), sample);
	
		// Now the value should be set. Setting it again with XX should succeed
		expect_string_result(client.set("redistest:no_exp", sample, c_invalid_duration, SetCondition::XX), "OK");

		// Delete the value again
		expect_int_result(client.del("redistest:no_exp"), 1);
		
		// And set it with an expiry time of one second
		expect_string_result(client.set("redistest:no_exp", sample, std::chrono::seconds(1)), "OK");
		expect_string_result(client.get("redistest:no_exp"), sample);

		// wait just over a second
		boost::this_thread::sleep_for(boost::chrono::milliseconds(1100));

		// and see if the value disappeared
		expect_null_result(client.get("redistest:no_exp"));

		// cleanup
		expect_some_result(client.del("redistest:no_exp"));
	
		return true;

	} catch (const moose::mredis::redis_error &merr) {
		std::cerr << "Error testing extended set parameters: " << boost::diagnostic_information(merr) << std::endl;
	} catch (...) {
		std::cerr << "Unhandled exception testing extended set parameters()" << std::endl;
	}

	return false;
}

// several test from when this was a new thing
bool test_hincr_by() {
	
	AsyncClient client(server_ip_string);
	client.connect();

	expect_some_result(client.hset("redistest:myhash", "field", "1"));

	expect_int_result(client.hincrby("redistest:myhash", "field", 1), 2);
	expect_int_result(client.hincrby("redistest:myhash", "field", 1), 3);
	expect_int_result(client.hincrby("redistest:myhash", "field", 1), 4);
	expect_int_result(client.hincrby("redistest:myhash", "field", 1), 5);
	expect_int_result(client.hincrby("redistest:myhash", "field", 1), 6);
	expect_int_result(client.hincrby("redistest:myhash", "field", 1), 7);

	std::cout << "Wait a sec... " << std::endl;
	boost::this_thread::sleep_for(boost::chrono::milliseconds(200));
	std::cout << "Again!" << std::endl;

	output_int_result(client.hincrby("redistest:myhash", "field", 1));
	output_int_result(client.hincrby("redistest:myhash", "field", 1));
	output_int_result(client.hincrby("redistest:myhash", "field", 1));
	output_int_result(client.hincrby("redistest:myhash", "field", 1));
	output_int_result(client.hincrby("redistest:myhash", "field", 1));
	output_int_result(client.hincrby("redistest:myhash", "field", 1));
	output_int_result(client.hincrby("redistest:myhash", "field", 1));

	client.hset("redistest:myhash", "testfield", "moep");
	client.hget("redistest:myhash", "testfield", [] (const RedisMessage &n_response) {

		expect_string_result(n_response, "moep");
	});

	boost::this_thread::sleep_for(boost::chrono::milliseconds(50));
	expect_some_result(client.set("redistest:myval:437!:test_key", "This is my Test!"));

	expect_string_result(client.get("redistest:myval:437!:test_key"), "This is my Test!");
	
	// cleanup
	expect_some_result(client.del("redistest:myval:437!:test_key"));
	expect_some_result(client.del("redistest:myhash"));
	expect_some_result(client.hdel("redistest:myhash", "testfield"));

	return true;
}

// see if we can really wait on a fiber
bool test_fibers() {
	
	// test fiber wait
	AsyncClient client(server_ip_string);
	client.connect();

	expect_string_result(client.set("redistest:fibertest_1", "Hello"), "OK");
	expect_string_result(client.set("redistest:fibertest_2", "World"), "OK");

	std::shared_ptr<FPromisedRedisMessage> prom1(std::make_shared<FPromisedRedisMessage>());
	std::shared_ptr<FPromisedRedisMessage> prom2(std::make_shared<FPromisedRedisMessage>());

	client.get("redistest:fibertest_1", [prom1] (const RedisMessage &n_message) {
		prom1->set_value(n_message);
	});

	client.get("redistest:fibertest_2", [prom2] (const RedisMessage &n_message) {
		prom2->set_value(n_message);
	});

	expect_string_result(prom2->get_future(), "World");
	expect_string_result(prom1->get_future(), "Hello");

	expect_some_result(client.del("redistest:fibertest_1"));
	expect_some_result(client.del("redistest:fibertest_2"));

	return true;
}

bool test_larger_binaries_mt() {

	const std::size_t num_keys = 10;

	AsyncClient client(server_ip_string);
	client.connect();

	// Better do a pre-run cleanup in case previous runs had leftovers
	for (unsigned int i = 0; i < num_keys; i++) {
		future_response res = client.del("redistest:" + std::to_string(i));
		if (res.wait_for(boost::chrono::seconds(15)) == boost::future_status::timeout) {
			BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout deleting binary value"));
		}
	}

	// prepare some samples to read and write later
	// They are stored as fixed values of random bytes in a map and then read and written to redis
	std::map<std::string, std::string> samples;

	for (unsigned int i = 0; i < num_keys; i++) {
			
		const boost::uint64_t byte_size = urand(64, 256);
		std::string sample;
		sample.reserve(byte_size);

		for (unsigned int n = 0; n < byte_size; n++) {
			sample.push_back(static_cast<char>(urand(0, 255)));
		}

		// Yes, keys are "0" to "9" as strings
		samples["redistest:" + std::to_string(i)] = sample;
	}

	std::atomic<bool> failed{ false };

	// start 10 threads that concurrently write and read those samples
	boost::thread_group workers;

	for (unsigned int i = 0; i < 10; i++) {

		workers.add_thread(new boost::thread{ [&] {

			try {

				const boost::chrono::steady_clock::time_point thread_start = boost::chrono::steady_clock::now();

				// for 30 seconds, we have 10 threads incrementing a value and checking the result or 
				// trying to cause a timeout, after which we are expected to recover.
				// Each failure to do so, causes the thread to end
				while (!failed && (boost::chrono::steady_clock::now() - thread_start) < boost::chrono::seconds(30)) {

					if (urand(1000) == 1) {
						std::cout << ".";
					}

					// either read or write
					if (urand(0, 1) == 0) {
						// write one sample
						const std::string key = "redistest:" + std::to_string(urand(0, num_keys - 1));

						// throw on not OK
						expect_string_result(client.set(key, samples[key]), "OK");

					} else {
						// read one sample
						const std::string key = "redistest:" + std::to_string(urand(0, num_keys - 1));

						future_response res = client.get(key);

						if (res.wait_for(boost::chrono::seconds(15)) == boost::future_status::timeout) {
							BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout getting binary value"));
						}

						RedisMessage response = res.get();

						if (is_null(response)) {
							// value not written yet
							continue;
						}

						if (is_string(response)) {
							const std::string str_res = boost::get<std::string>(response);
							const std::string original_sample = samples[key];

							// compare the string with the original in the map
							if (original_sample != str_res) {
								BOOST_THROW_EXCEPTION(redis_error() << error_message("Retrieved binary value does not match original sample"));
							}

						} else {
							BOOST_THROW_EXCEPTION(redis_error() << error_message("Retrieved binary value does not match original sample"));
						}
					}
				}

				std::cout << std::endl;

			} catch (const moose_error &merr) {
				std::cerr << "Exception in reading / writing large binaries: " << boost::diagnostic_information(merr);
				failed.store(true);
			}
		} } );
	}

	workers.join_all();

	// cleanup
	for (unsigned int i = 0; i < num_keys; i++) {
		future_response res = client.del("redistest:" + std::to_string(i));
		if (res.wait_for(boost::chrono::seconds(15)) == boost::future_status::timeout) {
			BOOST_THROW_EXCEPTION(redis_error() << error_message("Timeout deleting binary value"));
		}
	}

	return !failed;
}

using fsec = boost::chrono::duration<float>;

bool test_connection_timeout() {

	{
		const boost::chrono::steady_clock::time_point start = boost::chrono::steady_clock::now();

		try {
			std::cerr << "Testing sync connection timeout" << std::endl;
			
			// test connection timeout with our load balancer. It should filter redis port inbound,
			// causing our connection to time out
			AsyncClient client("TestingInbound-8a9215d5cf5207b9.elb.eu-central-1.amazonaws.com");

			// Should timeout after 2 seconds
			client.connect();

			// We do not want to be here
			std::cerr << "Sync connection timeout failed" << std::endl;
			return false;
			
		} catch (const moose::mredis::redis_error &merr) {
			
			const fsec dur = (boost::chrono::steady_clock::now() - start);
			if (dur > boost::chrono::milliseconds(1900) && dur < boost::chrono::milliseconds(2100)) {
				std::cout << "Connection timeout worked OK after " << dur.count() << " secs: " << merr.server_message() << std::endl;
			} else {
				std::cerr << "Connection timeout unexpected, exception after " << dur.count() << " secs: " << merr.server_message() << std::endl;
				return false;
			}
		}

		if ((boost::chrono::steady_clock::now() - start) > boost::chrono::seconds(3)) {
			std::cerr << "Connection timeout too long or not at all, perhaps you don't even see this" << std::endl;
		}
	}

	{
		std::cerr << "Testing async connection timeout" << std::endl;

		const boost::chrono::steady_clock::time_point start = boost::chrono::steady_clock::now();

		try {
			// Now let's try the same thing with async connect
			AsyncClient client("TestingInbound-8a9215d5cf5207b9.elb.eu-central-1.amazonaws.com");
			boost::shared_future<bool> ret = client.async_connect();

			// Wait for longer than the actual timeout would be, so I can assert than I should be able to get()
			if (ret.wait_for(boost::chrono::seconds(30)) == boost::future_status::timeout) {
				std::cerr << "Async connection timeout failed, future timed out" << std::endl;
				return false;
			}

			// this will throw if the connection times out
			const bool r = ret.get();
			boost::ignore_unused(r);

			// We do not want to be here
			std::cerr << "Async connection timeout failed" << std::endl;
			return false;

		} catch (const moose::mredis::redis_error &merr) {

			const fsec dur = (boost::chrono::steady_clock::now() - start);
			if (dur > boost::chrono::milliseconds(1900) && dur < boost::chrono::milliseconds(2100)) {
				std::cout << "Async connection timeout worked OK after " << dur.count() << " secs" << std::endl;
			} else {
				std::cerr << "Async connection timeout unexpected, exception after " << dur.count() << " secs: " << merr.server_message() << std::endl;
				return false;
			}
		}

		if ((boost::chrono::steady_clock::now() - start) > boost::chrono::seconds(3)) {
			std::cerr << "Async connection timeout too long or not at all, perhaps you don't even see this" << std::endl;
		}
	}

	return true;
}

// see if we can simulate read timeouts
bool test_read_timeout() {

	AsyncClient client(server_ip_string);
	client.connect();

	client.set("redistest:timeout_test_value", "Hello World!");

	// OK, now what is the desired behavior?
	// If a read timeout from the server occurs, I want the client object to continue to exist,
	// but close the connection and re-establish it.
	// Existing handlers, like the one that is waiting for response here should be 
	// salvaged and be called after the re-connect
	//
	// Alas, this won't be in the cards right now as I lack the time
	// and so all I can have now is to shutdown the connection and call the handler
	// with an error. Since I am using the retriever, I expect wait_for_response() 
	// to throw after 5 seconds
	//
	//
	boost::chrono::steady_clock::time_point start = boost::chrono::steady_clock::now();

	try {	
		// Set the wait for the response to 10 seconds
		BlockingRetriever< std::vector<RedisMessage> > sleep_getter{ 10 };

		// cause timeout after 5 (command will forcably take 6)
		client.debug_sleep(7, sleep_getter.responder());                  // read timeout is 5 seconds, so this should trigger it
		const boost::optional< std::vector<RedisMessage> > sr = sleep_getter.wait_for_response();
			
		// Now expect the client to have caused the error after approximately 5 seconds
		const fsec dur = (boost::chrono::steady_clock::now() - start);

		// Whatever that is, the retriever should have bailed with an exception after 10 seconds
		if (dur > boost::chrono::seconds(10)) {
			return false;
		}

		std::cerr << "Read timeout failed, returned after " << dur.count() << " secs" << std::endl;
		return false;
			
	} catch (const moose::mredis::redis_error &merr) {
		const fsec dur = (boost::chrono::steady_clock::now() - start);
		if (dur > boost::chrono::milliseconds(4500) && dur < boost::chrono::milliseconds(5500)) {
			std::cout << "Read timeout worked OK after " << dur.count() << " secs" << std::endl;
		} else {
			std::cerr << "Read timeout did not work, exception after " << dur.count() << " secs: " << merr.server_message() << std::endl;
			return false;
		}
	} catch (...) {
		std::cerr << "Unhandled exception caught. Read timeout failed" << std::endl;
		return false;
	}

	// So, what now?
	// Since I assume the client has now dropped its connection and I know the server should be OK
	// we expect the client to re-connect as the next command comes in
		
	// I'll do this by asking the server for the value we wrote earlier, which 
	// I expect the reconnect to be done in just over 2 secs, as the debug sleep above should have two left
	// hence the tight timeout
	start = boost::chrono::steady_clock::now();

	try {

		BlockingRetriever< std::string > value_getter{ 3 };
		client.get("redistest:timeout_test_value", value_getter.responder());
		const boost::optional<std::string> value = value_getter.wait_for_response();
			
		const fsec dur = (boost::chrono::steady_clock::now() - start);

		if (!value || value->empty()) {
			std::cerr << "No test value returned from Redis. Command returned after " << dur.count() << " secs" << std::endl;
			return false;
		} else {
			if (*value == "Hello World!") {

				if (dur > boost::chrono::milliseconds(1500) && dur < boost::chrono::milliseconds(2500)) {
					std::cout << "Reconnect worked OK after " << dur.count() << " secs" << std::endl;
				} else {
					std::cerr << "Reconnect after unexpected time of " << dur.count() << " secs" << std::endl;
					return false;
				}
			} else {
				std::cerr << "Test value did not check out: " << *value << std::endl;
				return false;
			} 
		}
	} catch (const moose::mredis::redis_error &merr) {
		const fsec dur = (boost::chrono::steady_clock::now() - start);
		std::cerr << "Reconnect failed after " << dur.count() << " seconds: " << merr.server_message() << std::endl;
		return false;
	} catch (...) {
		const fsec dur = (boost::chrono::steady_clock::now() - start);
		std::cerr << "Unhandled exception caught when reconnecting after " << dur.count() << " seconds" << std::endl;
		return false;
	}

	client.del("redistest:timeout_test_value");
	try {
		std::cout << "Testing recovered client" << std::endl;

		// Now do a few more sets and reads, which should now go quickly, as the connection should be stable now
		start = boost::chrono::steady_clock::now();

		client.set("redistest:testvalue", "42");
		expect_int_result(client.incr("redistest:testvalue"), 43);
		expect_int_result(client.incr("redistest:testvalue"), 44);
		expect_int_result(client.incr("redistest:testvalue"), 45);
		expect_int_result(client.incr("redistest:testvalue"), 46);
		expect_int_result(client.incr("redistest:testvalue"), 47);
		expect_int_result(client.incr("redistest:testvalue"), 48);
		client.del("redistest:testvalue");

		fsec duration = (boost::chrono::steady_clock::now() - start);
	
		if (duration < boost::chrono::milliseconds(200)) {
			std::cout << "Using reconnected client worked OK" << std::endl;
		} else {
			std::cerr << "Using reconnected client took unreasonably long: " << duration.count() << " secs" << std::endl;
			return false;
		}

		std::cout << "Sleeping this thread for 5 seconds" << std::endl;
		boost::this_thread::sleep_for(boost::chrono::seconds(5));
		std::cout << "Back to test it again" << std::endl;

		// Now do a few more sets and reads, which should now go quickly, as the connection should still be OK
		start = boost::chrono::steady_clock::now();

		client.set("redistest:testvalue", "42");
		expect_int_result(client.incr("redistest:testvalue"), 43);
		expect_int_result(client.incr("redistest:testvalue"), 44);
		expect_int_result(client.incr("redistest:testvalue"), 45);
		expect_int_result(client.incr("redistest:testvalue"), 46);
		expect_int_result(client.incr("redistest:testvalue"), 47);
		expect_int_result(client.incr("redistest:testvalue"), 48);
		client.del("redistest:testvalue");

		duration = (boost::chrono::steady_clock::now() - start);

		if (duration < boost::chrono::milliseconds(200)) {
			std::cout << "Using reconnected client again worked OK" << std::endl;
		} else {
			std::cerr << "Using reconnected client again took unreasonably long: " << duration.count() << " secs" << std::endl;
			return false;
		}

		std::cout << "Sleeping this thread for 3 more seconds" << std::endl;
		boost::this_thread::sleep_for(boost::chrono::seconds(5));
		std::cout << "Back to test it one last time" << std::endl;

		// Now do a few more sets and reads, which should now go quickly, as the connection should still be OK
		start = boost::chrono::steady_clock::now();

		client.set("redistest:testvalue", "23");
		expect_int_result(client.incr("redistest:testvalue"), 24);
		expect_int_result(client.incr("redistest:testvalue"), 25);
		expect_int_result(client.incr("redistest:testvalue"), 26);
		expect_int_result(client.incr("redistest:testvalue"), 27);
		expect_int_result(client.incr("redistest:testvalue"), 28);
		expect_int_result(client.incr("redistest:testvalue"), 29);
		client.del("redistest:testvalue");

		duration = (boost::chrono::steady_clock::now() - start);
		
		if (duration < boost::chrono::milliseconds(200)) {
			std::cout << "Using reconnected client again worked OK" << std::endl;
		} else {
			std::cerr << "Using reconnected client again took unreasonably long: " << duration.count() << " secs" << std::endl;
			return false;
		}

		return true;

	} catch (const moose::mredis::redis_error &merr) {
		std::cerr << "Error re-using client: " << boost::diagnostic_information(merr) << std::endl;
	} catch (...) {
		std::cerr << "Unhandled exception using recovered client" << std::endl;
	}
	
	return false;
}


// read timeouts in multi-thread usage
bool test_mt_read_timeout() {

	AsyncClient client(server_ip_string);
	client.connect();

	std::atomic<bool> success{ true };

	// I am aware that I would have to have a wider lock scope to actually 
	// have only one cause a timeout. This is not my intention. I should be able to stomach 
	// more anyway. I just intend to make it very likely only one does it to KISS it
	std::atomic<bool> timeout_caused{ false };

	const boost::chrono::steady_clock::time_point total_start = boost::chrono::steady_clock::now();

	client.set("redistest:mt:testval", "42");

	// start 10 threads that concurrently run a test similar to the single threaded read timeout test
	boost::thread_group workers;

	for (unsigned int i = 0; i < 20; i++) {

		workers.add_thread(new boost::thread{ [&] {

			const boost::chrono::steady_clock::time_point thread_start = boost::chrono::steady_clock::now();
			boost::int64_t current_value = 0;

			// for one minute, we have 10 threads incrementing a value and checking the result or 
			// trying to cause a timeout, after which we are expected to recover.
			// Each failure to do so, causes the thread to end
			while ((boost::chrono::steady_clock::now() - thread_start) < boost::chrono::seconds(60)) {

				if (moose::tools::urand(50000) == 1) {
					std::cout << "Thread " << boost::this_thread::get_id() << " ticking" << std::endl;
				}

				// In rare (.001%) of cases I want to cause a timeout, in all others I
				// just do an increment to cause traffic
				if (moose::tools::urand(100000) == 1) {

					if (timeout_caused) {
						continue;
					}
					
					std::cout << "Causing timeout" << std::endl;

					timeout_caused.store(true);

					const boost::chrono::steady_clock::time_point start = boost::chrono::steady_clock::now();

					try {
						// Set the wait for the response to 9 seconds
						BlockingRetriever< std::vector<RedisMessage> > sleep_getter{ 9 };
						client.debug_sleep(7, sleep_getter.responder());       // read timeout is 5 seconds, so this should trigger it
						const boost::optional< std::vector<RedisMessage> > sr = sleep_getter.wait_for_response();

						// Now expect the client to have caused the error after approximately 5 seconds
						// because this is when the timeout hits
						const fsec dur = (boost::chrono::steady_clock::now() - start);

						// Whatever that is, the retriever should have bailed with an exception after 9 seconds
						if (dur > boost::chrono::milliseconds(9500)) {
							std::cerr << "mt read timeout unexpectedly long after " << dur.count() << " secs" << std::endl;
							success.store(false);
							break;
						}

						// We are not supposed to arrive here
						std::cerr << "Read timeout failed, returned after " << dur.count() << " secs" << std::endl;
						success.store(false);
						break;

					} catch (const moose::mredis::redis_error &merr) {
						const fsec dur = (boost::chrono::steady_clock::now() - start);
						if (dur > boost::chrono::milliseconds(4500) && dur < boost::chrono::milliseconds(5500)) {
							std::cout << "Read timeout worked OK after " << dur.count() << " secs" << std::endl;
						} else {
							std::cerr << "Read timeout did not work, exception after " << dur.count() << " secs: " << merr.server_message() << std::endl;
							success.store(false);
							break;
						}
					} catch (...) {
						std::cerr << "Unhandled exception caught. Read timeout failed" << std::endl;
						success.store(false);
						break;
					}

				} else {
					// Normal traffic causing incr operation
					// Each incr operation is either:
					//   * within a few ms (normal)
					//   * read timeout hits after 5 seconds        (exception) (caused by endless wait)
					//   * getter hits wait timeout after 6 seconds (exception)
					const boost::chrono::steady_clock::time_point get_start = boost::chrono::steady_clock::now();
					try {				
						BlockingRetriever< boost::int64_t > incr_getter{ 6 };
						client.incr("redistest:mt:testval", incr_getter.responder());
						const boost::optional<  boost::int64_t > incr_result = incr_getter.wait_for_response();
						const fsec dur = (boost::chrono::steady_clock::now() - get_start);

						// Whatever that is, the retriever should have bailed with an exception after 10 seconds
						// I'll treat it as an extra error and break the loop
						if (dur > boost::chrono::milliseconds(6500)) {
							std::cerr << "mt read or timeout failed after " << dur.count() << " secs" << std::endl;
							success.store(false);
							break;
						}

						if (!incr_result) {
							std::cerr << "mt returned without a value after " << dur.count() << " secs" << std::endl;
							success.store(false);
							break;
						} else {
							if (*incr_result <= current_value) {
								std::cerr << "incr result not larger than last time: " << *incr_result << " was: " << current_value << std::endl;
								success.store(false);
								break;
							} else {
								// This should be the 'normal' case
								current_value = *incr_result;
							}
						}

					} catch (const moose::mredis::redis_error &merr) {
						const fsec dur = (boost::chrono::steady_clock::now() - get_start);
						if (dur < boost::chrono::milliseconds(5500)) {
							std::cout << "Read timeout worked OK after " << dur.count() << " secs" << std::endl;
						} else {
							std::cerr << "Read timeout did not work, exception after " << dur.count() << " secs: " << merr.server_message() << std::endl;
							success.store(false);
							break;
						}
					} catch (...) {
						std::cerr << "Unhandled exception caught. Read timeout failed" << std::endl;
						success.store(false);
						break;
					}
				}

			}
		} });
	}

	workers.join_all();

	const fsec total_duration = (boost::chrono::steady_clock::now() - total_start);
	std::cout << "All threads joined after " << total_duration.count() << " seconds" << std::endl;

	{
		// new client for cleanup as we don't know in what state the original one is now
		AsyncClient cleaner(server_ip_string);
		cleaner.connect();
		cleaner.del("redistest:mt:testval");
	}

	if (!success) {
		std::cerr << "One or more threads in MT test reported an error" << std::endl;
		return false;
	}

	return true;
}

// see if we can deal with log running operations. I use a script to simulate this
void test_long_runs() {

	// test fiber wait
	AsyncClient client(server_ip_string);
	client.connect();

	// I set some other value to have something to retrieve later
	client.set("answer", "42");

	const std::vector<std::string> keys{};
	const std::vector<std::string> args{
		"10000000000"                                 // loops
	};

	const std::string time_wasting_script{
		"local cnt = 42 "
		"local ret = 0 "
		"for i = 0,tonumber(ARGV[1]),1 do "
			"cnt = cnt * i "
			"cnt = cnt / 2 "
			"ret = ret + cnt * i "
		"end "
		"return ret "
	};

	BlockingRetriever<boost::int64_t> rtr(1);   // timeout of one second should be busted

	try {
		client.eval(time_wasting_script, keys, args, rtr.responder());
		const boost::optional<boost::int64_t> result = rtr.wait_for_response();

		// I don't want to see either message but the timeout to cause an exception
		if (!result) {
			std::cerr << "No result from endless loop" << *result << std::endl;
		} else {
			std::cout << "Endless loop returned " << *result << std::endl;
		}
	} catch (const moose::mredis::redis_error &merr) {
		std::cout << "Endless caused exception. Now try to recover the client. " << merr.server_message() << std::endl;
	}


	// #moep #performance
	// This is not done yet. The server is now in a state that it won't accept 
	// anything other than treating the endless script by either killing it for forcefully shutting down the server.
	// I don't know yet what and how I should implement

	BlockingRetriever<boost::int64_t> dummy_getter(1);   // timeout of one second should be enough
	client.get("answer", dummy_getter.responder());
	const boost::optional<boost::int64_t> result = dummy_getter.wait_for_response();
	boost::ignore_unused(result);

	client.del("answer");

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
		("omit,o", "if set, long running tests such as timeouts will be omitted, otherwise (default) performed.")
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

		const bool perform_long_running_tests = !vm.count("omit");
		server_ip_string = vm["server"].as<std::string>();


		std::cout << "===========================================" << std::endl;
		std::cout << "Testing getter and setter" << std::endl;
		std::cout << "===========================================" << std::endl;
		if (!test_binary_get()) {
			std::cerr << "Binary getter and setter failed. Bailing..." << std::endl;
			return EXIT_FAILURE;
		}
		std::cout << "===========================================" << std::endl;
		std::cout << "Binary getter and setter successful"         << std::endl;
		std::cout << "===========================================" << std::endl;

		std::cout << "===========================================" << std::endl;
		std::cout << "Testing extended set parameters" << std::endl;
		std::cout << "===========================================" << std::endl;
		if (!test_extended_set_params()) {
			std::cerr << "Extended set parameters failed. Bailing..." << std::endl;
			return EXIT_FAILURE;
		}
		std::cout << "===========================================" << std::endl;
		std::cout << "Extended set parameters successful"          << std::endl;
		std::cout << "===========================================" << std::endl;

		std::cout << "===========================================" << std::endl;
		std::cout << "Testing Lua eval" << std::endl;
		std::cout << "===========================================" << std::endl;
		if (!test_lua()) {
			std::cerr << "Lua test suite failed. Bailing..." << std::endl;
			return EXIT_FAILURE;
		}
		std::cout << "===========================================" << std::endl;
		std::cout << "Lua test suite successful" << std::endl;
		std::cout << "===========================================" << std::endl;

		std::cout << "===========================================" << std::endl;
		std::cout << "Testing incr_by" << std::endl;
		std::cout << "===========================================" << std::endl;
		if (!test_hincr_by()) {
			std::cerr << "Incrby test suite failed. Bailing..." << std::endl;
			return EXIT_FAILURE;
		}
		std::cout << "===========================================" << std::endl;
		std::cout << "Incrby test suite successful" << std::endl;
		std::cout << "===========================================" << std::endl;

		std::cout << "===========================================" << std::endl;
		std::cout << "Testing fibers getter" << std::endl;
		std::cout << "===========================================" << std::endl;
		if (!test_fibers()) {
			std::cerr << "Fibers getter suite failed. Bailing..." << std::endl;
			return EXIT_FAILURE;
		}
		std::cout << "===========================================" << std::endl;
		std::cout << "Fibers getter test successful" << std::endl;
		std::cout << "===========================================" << std::endl;

		if (perform_long_running_tests) {
		
			std::cout << "===========================================" << std::endl;
			std::cout << "Testing larger binaries mt" << std::endl;
			std::cout << "===========================================" << std::endl;
			if (test_larger_binaries_mt()) {
				std::cout << "===========================================" << std::endl;
				std::cout << "Large binaries test suite successful" << std::endl;
				std::cout << "===========================================" << std::endl;
			} else {
				std::cerr << "Large binaries test suite failed. Bailing..." << std::endl;
				return EXIT_FAILURE;
			}

			std::cout << "===========================================" << std::endl;
			std::cout << "Testing connection timeout" << std::endl;
			std::cout << "===========================================" << std::endl;
			if (test_connection_timeout()) {
				std::cout << "===========================================" << std::endl;
				std::cout << "Connection timeout test suite successful" << std::endl;
				std::cout << "===========================================" << std::endl;
			} else {
				std::cerr << "Connection timeout test suite failed. Bailing..." << std::endl;
				return EXIT_FAILURE;
			}

			std::cout << "===========================================" << std::endl;
			std::cout << "Testing read timeout" << std::endl;
			std::cout << "===========================================" << std::endl;
			if (test_read_timeout()) {
				std::cout << "===========================================" << std::endl;
				std::cout << "Read timeout test suite successful" << std::endl;
				std::cout << "===========================================" << std::endl;
			} else {
				std::cerr << "Read timeout test suite failed. Bailing..." << std::endl;
				return EXIT_FAILURE;
			}

			std::cout << "===========================================" << std::endl;
			std::cout << "Testing multithreaded read timeout" << std::endl;
			std::cout << "===========================================" << std::endl;
			if (test_mt_read_timeout()) {
				std::cout << "===========================================" << std::endl;
				std::cout << "Multithreaded reconnect test suite successful" << std::endl;
				std::cout << "===========================================" << std::endl;
			} else {
				std::cerr << "Multithreaded reconnect test suite failed. Bailing..." << std::endl;
				return EXIT_FAILURE;
			}
		}

		std::cout << "done, all tests passed" << std::endl;

		return EXIT_SUCCESS;

	} catch (const moose_error &merr) {
		std::cerr << "Exception executing test cases: " << boost::diagnostic_information(merr) << std::endl;
	} catch (const std::exception &sex) {
		std::cerr << "Unexpected exception reached main: " << sex.what() << std::endl;
	} catch (...) {
		std::cerr << "Unhandled error reached main function. Aborting" << std::endl;
	}

	return EXIT_FAILURE;
}
