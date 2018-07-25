
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "mredis/AsyncClient.hpp"

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

int main(int argc, char **argv) {
	
#ifndef _WIN32
	// core dumps may be disallowed by parent of this process; change that
	struct rlimit core_limits;
	core_limits.rlim_cur = core_limits.rlim_max = RLIM_INFINITY;
	setrlimit(RLIMIT_CORE, &core_limits);
#endif


	namespace po = boost::program_options;

	po::options_description desc("redis test options");
	desc.add_options()
		("help,h", "Print this help message");
// 		("port,p", po::value<short>()->default_value(7777), "use server port")
// 		("server,s", po::value<std::string>()->default_value("127.0.0.1"), "give server ip");

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
// 		const std::string server_ip_string = vm["server"].as<std::string>();
// 		const short port = vm["port"].as<short>();

		boost::asio::io_context io_ctx;
		boost::asio::io_context::work *work = new boost::asio::io_context::work(io_ctx);
		std::unique_ptr<boost::thread> t(new boost::thread([&]() { io_ctx.run(); }));


		AsyncClient client(io_ctx, "127.0.0.1");

		client.connect();
		boost::this_thread::sleep_for(boost::chrono::seconds(1));

		client.hincrby("myhash", "field", 1, [](const RESPonse &n_response) {
			
			if (n_response.which() == 2) {
				std::cout << "Response: " << boost::get<boost::int64_t>(n_response) << std::endl;
			}
		});



		boost::this_thread::sleep_for(boost::chrono::seconds(1));

		delete work;
		io_ctx.stop();
		t->join();



	} catch (const std::exception &sex) {
		std::cerr << "Unexpected exception reached main: " << sex.what() << std::endl;
	} catch (...) {
		std::cerr << "Unhandled error reached main function. Aborting" << std::endl;
	}

	return EXIT_SUCCESS;
}
