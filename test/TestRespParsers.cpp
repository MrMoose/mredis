
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
	RedisMessage r = parse_one(is);

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
	RedisMessage r = parse_one(is);

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
	RedisMessage r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_CHECK(r.which() == 4);

	const std::vector<RedisMessage> results = boost::get<std::vector<RedisMessage> >(r);

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
	RedisMessage r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_REQUIRE(r.which() == 3);
}

bool require_bulk_string(const RedisMessage &n_message, const std::string &n_value) {
	
	if (n_message.which() != 1) {
		return false;
	}

	if (n_value != boost::get<std::string>(n_message)) {
		return false;
	}

	return true;
}

BOOST_AUTO_TEST_CASE(Bulk) {

	boost::asio::streambuf sb;
	std::string sample("$5\r\nH\0llo\r\n", 11);

	{
		// serialize the sample
		std::ostream os(&sb);
		BOOST_CHECK_NO_THROW(generate_to_stream(os, sample));
	}
	{
		// and read it back in
		std::istream is(&sb);
		RedisMessage msg;
		BOOST_REQUIRE(parse_from_stream(is, msg));
		BOOST_CHECK(require_bulk_string(msg, sample));
	}
}

BOOST_AUTO_TEST_CASE(NullString) {

	boost::asio::streambuf sb;
	{
		std::ostream os(&sb);
		os.write("$0\r\n\r\n", 6); // null byte answer
	}

	std::istream is(&sb);
	RedisMessage r;
	BOOST_CHECK(parse_from_stream(is, r));

	BOOST_CHECK(r.which() == 1);

	const std::string result = boost::get<std::string>(r);

	BOOST_CHECK(result.size() == 0);
	BOOST_CHECK(result.empty());
}

BOOST_AUTO_TEST_CASE(ArraySerialize) {

	boost::asio::streambuf sb;

	// Assemble an array of assorted values
	std::vector<RedisMessage> arr;
	
	arr.push_back("Hello World");
	arr.push_back(null_result());
	arr.push_back(42);
	arr.push_back(std::string("Test C\0mplete", 13));

	{
		// serialize the array
		std::ostream os(&sb);
		BOOST_CHECK_NO_THROW(generate_to_stream(os, arr));
	}
	{
		// and read it back in
		std::istream is(&sb);
		RedisMessage msg;
		BOOST_REQUIRE(parse_from_stream(is, msg));

		BOOST_REQUIRE(msg.which() == 4);
		const std::vector<RedisMessage> res = boost::get<std::vector<RedisMessage> >(msg);

		BOOST_CHECK(res.size() == 4);
		BOOST_CHECK(res[0].which() == 1);
		BOOST_CHECK(res[1].which() == 3);
		BOOST_CHECK(res[2].which() == 2);
		BOOST_CHECK(res[3].which() == 1);

		BOOST_CHECK(boost::get<std::string>(res[0]) == "Hello World");
		BOOST_CHECK(boost::get<boost::int64_t>(res[2]) == 42);
		BOOST_CHECK(boost::get<std::string>(res[3]) == std::string("Test C\0mplete", 13));
	}
}

