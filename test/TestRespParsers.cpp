
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#define BOOST_TEST_MODULE RespParsersTest
#include <boost/test/unit_test.hpp>

#include "../RESP.hpp"

#include <boost/iostreams/stream.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/asio/streambuf.hpp>

#include <iostream>
#include <string>

using namespace moose::mredis;

BOOST_AUTO_TEST_CASE(Message) {

}

BOOST_AUTO_TEST_CASE(Pong) {

	// use a streambuf scenario close to what we intend
	boost::asio::streambuf sb;

	{
		std::ostream os(&sb);
		os << "+PONG\r\n";
	}

	std::istream is(&sb);
	RESPonse r = parse_one(is);

	BOOST_CHECK(r.which() == 1);
	BOOST_CHECK(boost::get<std::string>(r) == "PONG");
}
