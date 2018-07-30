
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

BOOST_AUTO_TEST_CASE(Error) {

	// use a streambuf scenario close to what we intend
	boost::asio::streambuf sb;

	{
		std::ostream os(&sb);
		os << "-ERR something\r\n";
	}

	std::istream is(&sb);
	RESPonse r = parse_one(is);

	BOOST_CHECK(r.which() == 0);

	redis_error e = boost::get<redis_error>(r);

	const std::string *server_message = boost::get_error_info<redis_server_message>(e);
	
	BOOST_REQUIRE(server_message);
	BOOST_CHECK(*server_message == "ERR something");
}

BOOST_AUTO_TEST_CASE(Array) {

	// use a streambuf scenario close to what we intend
	boost::asio::streambuf sb;

	{
		std::ostream os(&sb);
		os << "*3\r\n+String\r\n:42\r\n+Nocheinstring\r\n";
	}

	std::istream is(&sb);
	RESPonse r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_CHECK(r.which() == 4);

	const std::vector<RESPonse> results = boost::get<std::vector<RESPonse>>(r);

	BOOST_CHECK(results.size() == 3);
	BOOST_CHECK(results[0].which() == 1);
	BOOST_CHECK(results[1].which() == 2);
	BOOST_CHECK(results[2].which() == 1);

	BOOST_CHECK(boost::get<std::string>(results[0]) == "String");
	BOOST_CHECK(boost::get<boost::int64_t>(results[1]) == 42);
	BOOST_CHECK(boost::get<std::string>(results[2]) == "Nocheinstring");
}

BOOST_AUTO_TEST_CASE(Null) {

	// use a streambuf scenario close to what we intend
	boost::asio::streambuf sb;

	{
		std::ostream os(&sb);
		os << "$-1\r\n";
	}

	std::istream is(&sb);
	RESPonse r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_REQUIRE(r.which() == 3);
}

BOOST_AUTO_TEST_CASE(Bulk) {

	boost::asio::streambuf sb;

	{
		std::ostream os(&sb);
		os.write("$5\r\nH\0llo\r\n", 11);  //  Basically "Hello" but with the 'e' replaced by null
	}

	std::istream is(&sb);
	RESPonse r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_REQUIRE(r.which() == 1);

	const std::string result = boost::get<std::string>(r);

	BOOST_CHECK(result.size() == 5);
}

BOOST_AUTO_TEST_CASE(NullString) {

	boost::asio::streambuf sb;
	{
		std::ostream os(&sb);
		os.write("$0\r\n\r\n", 6); // null byte answer
	}

	std::istream is(&sb);
	RESPonse r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_CHECK(r.which() == 1);

	const std::string result = boost::get<std::string>(r);

	BOOST_CHECK(result.size() == 0);
	BOOST_CHECK(result.empty());
}
