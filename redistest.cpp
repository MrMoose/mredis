
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "mredis/AsyncClient.hpp"

#include "tools/Log.hpp"
#include "tools/Error.hpp"

#include <boost/config.hpp>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/program_options.hpp>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <chrono>
#include <iostream>
#include <cstdlib>
#include <string>

using namespace moose::mredis;
using namespace moose::tools;

std::string server_ip_string;

void output_int_result(future_response &&n_response) {

	RedisMessage response = n_response.get();

	if (response.which() == 2) {
		std::cout << "Response: " << boost::get<boost::int64_t>(response) << std::endl;
	} else {
		std::cerr << "Unexpected response: " << response.which() << std::endl;
	};
}

void expect_string_result(future_response &&n_response) {

	RedisMessage response = n_response.get();

	if (response.which() == 1) {
		std::cout << "Got string response: " << boost::get<std::string>(response) << std::endl;
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not a string response"));
	};
}

void expect_string_result(future_response &&n_response, const std::string &n_expected_string) {

	RedisMessage response = n_response.get();

	if (response.which() == 1) {
		if (boost::get<std::string>(response) != n_expected_string) {
			BOOST_THROW_EXCEPTION(redis_error() << error_message("Unexpected string response")
					<< error_argument(boost::get<std::string>(response)));
		}
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not a string response"));
	};
}

void expect_null_result(future_response &&n_response) {

	RedisMessage response = n_response.get();

	if (response.which() == 3) {
		std::cout << "Got expected null response" << std::endl;
	} else {
		BOOST_THROW_EXCEPTION(redis_error() << error_message("Not a null response"));
	};
}

void test_binary_get() {
	
	// Test getting and setting a binary value with at least one null byte in it.
	const std::string binary_sample("Hello\0 World", 12);

	AsyncClient client(server_ip_string);
	client.connect();
	client.set("myval:437!:bin_test_key", binary_sample);

	future_response sr1 = client.get("myval:437!:bin_test_key");
	RedisMessage br1 = sr1.get();

	// I expect the response to be a string containing the same binary value
	if (br1.which() != 1) {
		std::cerr << "not a string response: " << br1.which() << std::endl;
	} else {
		if (binary_sample != boost::get<std::string>(br1)) {
			std::cerr << "Binary set failed: " << boost::get<std::string>(br1) << std::endl;
		}
	}
}

// test the eval command a bit. I'll try not to actually test Lua as this is supposed to 
// be way out of scope. So I'll keep the actual scripts minimal
void test_lua() {

	AsyncClient client(server_ip_string);
	client.connect();

	// Very simple set
	expect_string_result(
		client.eval("return redis.call('set', 'foo', 'bar')", std::vector<LuaArgument>()),
		"OK"
	);

	// Set with binary arguments
	LuaArgument arg1(std::string("Hel\r\nlo", 7), std::string("W\0rld", 5));
	expect_string_result(
		client.eval("return redis.call('set', KEYS[1], ARGV[1])", arg1),
		"OK"
	);

	// get the binary string back using regular get and expect to be same
	expect_string_result(client.get(std::string("Hel\r\nlo", 7)), std::string("W\0rld", 5));
	

	// a little more complex script that increases a number of seats and occupies one if available
	client.set("used_seats", "3");

	const std::string add_seat( // It appears as if everything is type-less stored as string. 
		                        // In order to make Lua know I intend to treat it as a number, I have
		                        // to explicitly use tonumber()
		"local used_seats = tonumber(redis.call('get', 'used_seats'))"
		"if used_seats < 4 then                                      "
		"    redis.call('incr', 'used_seats')                        "
		"    redis.call('set', KEYS[1], ARGV[1])                     "
		"    return 'OK'                                             "
		"else                                                        "
		"    return nil                                              "
		"end"
	);

	// This should work once
	expect_string_result(client.eval(add_seat, LuaArgument("seat4", "Moose")), "OK");

	// But not again
	expect_null_result(client.eval(add_seat, LuaArgument("seat5", "PoorBugger")));

	// Because all seats are used
	//expect_int_result(client.eval(add_seat, LuaArgument("seat5", "PoorBugger")));

	// cleanup
	client.del("seat4");
	client.del(std::string("Hel\r\nlo", 7));
	client.del("foo");
}


// Test setting a value with additional parameters
void test_extended_set_params() {

	// Test getting and setting a binary value with at least one null byte in it.
	const std::string sample("Hello World!");

	AsyncClient client(server_ip_string);
	client.connect();

	// Delete possibly existing test value
	client.del("no_exp");

	// First try to set value without XX should fail
	expect_null_result(client.set("no_exp", sample, c_invalid_duration, SetCondition::XX));
	
	// Now set the value with NX should succeed
	expect_string_result(client.set("no_exp", sample, c_invalid_duration, SetCondition::NX), "OK");

	// Check the value
	expect_string_result(client.get("no_exp"), sample);
	
	// Now the value should be set. Setting it again with XX should succeed
	expect_string_result(client.set("no_exp", sample, c_invalid_duration, SetCondition::XX), "OK");

	// Delete the value again
	client.del("no_exp");

	// And set it with an expiry time of one second
	expect_string_result(client.set("no_exp", sample, std::chrono::seconds(1)), "OK");
	expect_string_result(client.get("no_exp"), sample);

	// wait just over a second
	boost::this_thread::sleep_for(boost::chrono::milliseconds(1100));

	// and see if the value disappeared
	expect_null_result(client.get("no_exp"));
}

// several test from when this was a new thing
void test_hincr_by() {
	
	AsyncClient client(server_ip_string);
	client.connect();

	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));

	std::cout << "Wait a sec... " << std::endl;
	boost::this_thread::sleep_for(boost::chrono::milliseconds(200));
	std::cout << "Again!" << std::endl;

	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));
	output_int_result(client.hincrby("myhash", "field", 1));

	client.hset("myhash", "testfield", "moep");
	client.hget("myhash", "testfield", [] (const RedisMessage &n_response) {

		// I expect the response to be a string containing a simple date time format
		if (n_response.which() != 1) {
			std::cerr << "not a string response: " << n_response.which();
		} else {
			std::cout << "Response: " << boost::get<std::string>(n_response) << std::endl;
		}
	});

	boost::this_thread::sleep_for(boost::chrono::milliseconds(50));
	client.set("myval:437!:test_key", "This is my Test!");

	expect_string_result(client.get("myval:437!:test_key"), "This is my Test!");
	
	// cleanup
	client.del("myval:437!:test_key");
	client.del("myhash");
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

		test_binary_get();

		test_extended_set_params();

		test_lua();

		test_hincr_by();


		std::cout << "done" << std::endl;

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
