
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "mredis/AsyncClient.hpp"

#include "tools/Log.hpp"

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

using namespace moose::mredis;

void output_result(future_response &n_response) {

	RESPonse response = n_response.get();

	if (response.which() == 2) {
		std::cout << "Response: " << boost::get<boost::int64_t>(response) << std::endl;
	} else {
		std::cerr << "Unexpected response: " << response.which() << std::endl;
	};
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

		boost::asio::io_context io_ctx;
		boost::asio::io_context::work *work = new boost::asio::io_context::work(io_ctx);
		std::unique_ptr<boost::thread> t(new boost::thread([&]() { io_ctx.run(); }));


		{

			AsyncClient client(io_ctx, server_ip_string);

			client.connect();

			future_response fr1 = client.hincrby("myhash", "field", 1);
			future_response fr2 = client.hincrby("myhash", "field", 1);
			future_response fr3 = client.hincrby("myhash", "field", 1);
			future_response fr4 = client.hincrby("myhash", "field", 1);
			future_response fr5 = client.hincrby("myhash", "field", 1);
			future_response fr6 = client.hincrby("myhash", "field", 1);
			future_response fr7 = client.hincrby("myhash", "field", 1);

			output_result(fr1);
			output_result(fr2);
			output_result(fr3);
			output_result(fr4);
			output_result(fr5);
			output_result(fr6);
			output_result(fr7);

			std::cout << "Wait a sec... " << std::endl;
			boost::this_thread::sleep_for(boost::chrono::seconds(1));
			std::cout << "Again!" << std::endl;

			future_response fr8 = client.hincrby("myhash", "field", 1);
			future_response fr9 = client.hincrby("myhash", "field", 1);
			future_response fr10 = client.hincrby("myhash", "field", 1);
			future_response fr11 = client.hincrby("myhash", "field", 1);
			future_response fr12 = client.hincrby("myhash", "field", 1);
			future_response fr13 = client.hincrby("myhash", "field", 1);
			future_response fr14 = client.hincrby("myhash", "field", 1);

			output_result(fr8);
			output_result(fr9);
			output_result(fr10);
			output_result(fr11);
			output_result(fr12);
			output_result(fr13);
			output_result(fr14);

			client.hset("myhash", "testfield", "moep");
			client.hget("myhash", "testfield", [](const RESPonse &n_response) {
			
				// I expect the response to be a string containing a simple date time format
				if (n_response.which() != 1) {
					std::cerr << "not a string response: " << n_response.which();
				} else {
					std::cout << "Response: " << boost::get<std::string>(n_response) << std::endl;
				}
			});

			boost::this_thread::sleep_for(boost::chrono::milliseconds(50));
			client.set("myval:437!:test_key", "This is my Test!");
			
			future_response sr1 = client.get("myval:437!:test_key");

			RESPonse srr1 = sr1.get();
				
			// I expect the response to be a string containing a simple date time format
			if (srr1.which() != 1) {
				std::cerr << "not a string response: " << srr1.which() << std::endl;
			} else {
				std::cout << "Response string get: " << boost::get<std::string>(srr1) << std::endl;
			}
			
		}


		std::cout << "shutting down" << std::endl;
		delete work;
		io_ctx.stop();
		t->join();

		std::cout << "done" << std::endl;

	} catch (const std::exception &sex) {
		std::cerr << "Unexpected exception reached main: " << sex.what() << std::endl;
	} catch (...) {
		std::cerr << "Unhandled error reached main function. Aborting" << std::endl;
	}

	return EXIT_SUCCESS;
}
