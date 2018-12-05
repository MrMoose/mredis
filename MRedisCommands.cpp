
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "MRedisCommands.hpp"
#include "RESP.hpp"

#include "tools/Assert.hpp"

#include <boost/asio.hpp>
#include <boost/spirit/include/karma.hpp>
#include <boost/spirit/include/karma_format.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/chrono.hpp>

namespace moose {
namespace mredis {

namespace karma = boost::spirit::karma;
namespace phx = boost::phoenix;

using karma::no_delimit;
using karma::repeat;
using karma::uint_;
using karma::byte_;
using karma::string;
using karma::lit;

void format_ping(std::ostream &n_os) {

	n_os << karma::format("PING\r\n");
}

void format_get(std::ostream &n_os, const std::string &n_key) {

	n_os << karma::format_delimited(
		lit("*2") <<                // Array of 2 fields...
		lit("$3") <<                // Bulk string of length 3 (length of the term "GET")
		lit("GET") <<               // Get command
		no_delimit['$'] << uint_ << // binary length of key
		string                      // key
		, "\r\n", n_key.size(), n_key);
}

void format_set(std::ostream &n_os, const std::string &n_key, const std::string &n_value,
		const Duration &n_expire_time, const SetCondition n_condition) {

	// naive approach
	// This will fail once the string contains null bytes or hyphens or anything else that needs quoting
//	n_os << karma::format_delimited("SET" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
//		" ", n_key, n_value);

	unsigned int num_fields = 3;
	std::string expire_time_str;
	
	if (n_expire_time != c_invalid_duration) {
		// I need to pre-format this because I can't otherwise know the length in advance
		expire_time_str.reserve(16);
		std::back_insert_iterator<std::string> out(expire_time_str);
		karma::generate(out, uint_, boost::chrono::duration_cast<boost::chrono::seconds>(n_expire_time).count());
		num_fields += 2;
	}
	
	if (n_condition != SetCondition::NONE) {
		num_fields++;
	}

	// Protocol expects a bulk string to know its length in advance.

	// sending everything as bulk strings prevents that from being a problem
	// What I don't know is: Are there any disadvantages of always using the bulk string approach?
	// Why would I ever choose the former, except for simplicity and documentation purposes?
	n_os << karma::format_delimited(
		no_delimit['*'] << uint_ << // Array of how many fields...
		lit("$3") <<                // Bulk string of length 3  (length of the term "SET")
		lit("SET") <<               // set command
		no_delimit['$'] << uint_ << // binary length of key
		string <<                   // key
		no_delimit['$'] << uint_ << // binary length of value
		string                      // value
		, "\r\n", num_fields, n_key.size(), n_key, n_value.size(), n_value);

	if (n_expire_time != c_invalid_duration) {
		n_os << karma::format_delimited(
			lit("$2") <<                 // Bulk string of length 2 for "EX"
			lit("EX") <<
			no_delimit['$'] << uint_ <<  // Bulk string for expiry time
			string
			, "\r\n", expire_time_str.size(), expire_time_str);
	}

	switch (n_condition) {
		default:
		case SetCondition::NONE:
			break;
		case SetCondition::NX:
			n_os.write("$2\r\nNX\r\n", 8);
			break;
		case SetCondition::XX:
			n_os.write("$2\r\nXX\r\n", 8);
			break;
	}
}

void format_del(std::ostream &n_os, const std::string &n_key) {
	
	n_os << karma::format_delimited(
		lit("*2") <<                // Array of 2 fields...
		lit("$3") <<                // Bulk string of length 3  (length of the term "SET")
		lit("DEL") <<               // set command
		no_delimit['$'] << uint_ << // binary length of key
		string                      // key
		, "\r\n", n_key.size(), n_key);
}

void format_exists(std::ostream &n_os, const std::string &n_key) {
	
	n_os << karma::format_delimited(
		lit("*2") <<                // Array of 2 fields...
		lit("$6") <<                // Bulk string of length 6  (length of the term "EXISTS")
		lit("EXISTS") <<            // exists command
		no_delimit['$'] << uint_ << // binary length of key
		string                      // key
		, "\r\n", n_key.size(), n_key);
}

void format_incr(std::ostream &n_os, const std::string &n_key) {
	
	n_os << karma::format("INCR \"" << karma::string << "\"\r\n", n_key);
}

void format_decr(std::ostream &n_os, const std::string &n_key) {

	n_os << karma::format("DECR \"" << karma::string << "\"\r\n", n_key);
}

void format_hincrby(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name, const boost::int64_t n_incr_by) {

	n_os << karma::format_delimited("HINCRBY" << karma::string << karma::string << karma::long_long << karma::no_delimit["\r\n"],
			" ", n_hash_name, n_field_name, n_incr_by);
}

void format_hget(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name) {

	n_os << karma::format_delimited("HGET" << karma::string << karma::string << karma::no_delimit["\r\n"],
			" ", n_hash_name, n_field_name);
}

void format_hset(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name, const std::string &n_value) {

	n_os << karma::format_delimited("HSET" << karma::string << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_hash_name, n_field_name, n_value);
}

void format_lpush(std::ostream &n_os, const std::string &n_list_name, const std::string &n_value) {

	n_os << karma::format_delimited("LPUSH" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_list_name, n_value);
}

void format_rpush(std::ostream &n_os, const std::string &n_list_name, const std::string &n_value) {

	n_os << karma::format_delimited("RPUSH" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_list_name, n_value);
}

void format_sadd(std::ostream &n_os, const std::string &n_set_name, const std::string &n_value) {

	n_os << karma::format_delimited("SADD" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_set_name, n_value);
}

void format_eval(std::ostream &n_os, const std::string &n_script, const std::vector<std::string> &n_keys, const std::vector<std::string> &n_args) {

	// This is Lua script eval. I am assuming the script doesn't contain anything weird (null bytes)
	// It does however contain lots of newlines.
	// The keys and values however might contain binary. So I try the bulk approach first

	// According to how I read the protocol specs Redis should accept arrays of mixed types
	// However whenever I do it I get a protocol error. So I convert the number of arguments
	// to a string first. This should not be necessary

	std::string num_keys_str;
	std::back_insert_iterator<std::string> out(num_keys_str);
	karma::generate(out, uint_, n_keys.size());

	const std::size_t num_fields = 3    // EVAL $SCRIPT $NUMBER_OF_KEYS ...
			+ n_keys.size()             // how many keys
			+ n_args.size();            // how many keys arguments

	n_os << karma::format_delimited(
		no_delimit['*'] << uint_ << // Array of how many fields...
		lit("$4") <<                // Bulk string of length 4 (length of the term "EVAL")
		lit("EVAL") <<              // eval command
		no_delimit['$'] << uint_ << // binary length of script
		string <<                   // script
		no_delimit['$'] << uint_ << // length of number of keys string
		string                      // number of keys string
		, "\r\n", num_fields, n_script.size(), n_script, num_keys_str.size(), num_keys_str);

	// First all the keys
	for (const std::string &key: n_keys) {
		n_os << karma::format_delimited(
			no_delimit['$'] << uint_ << // binary length of script
			string                      // script
			, "\r\n", key.size(), key);
	}

	// Now again for all the values
	for (const std::string &arg : n_args) {
		n_os << karma::format_delimited(
			no_delimit['$'] << uint_ << // binary length of script
			string                      // script
			, "\r\n", arg.size(), arg);
	}
}

void format_subscribe(std::ostream &n_os, const std::string &n_channel_name) {
	
	n_os << karma::format_delimited("SUBSCRIBE MREDIS_WAKEUP" << karma::no_delimit[karma::string << "\r\n"],
			" ", n_channel_name);
}

void format_unsubscribe(std::ostream &n_os, const std::string &n_channel_name) {

	n_os << karma::format_delimited("UNSUBSCRIBE" << karma::no_delimit[karma::string << "\r\n"],
			" ", n_channel_name);
}

void format_publish(std::ostream &n_os, const std::string &n_channel_name, const std::string &n_message) {

	n_os << karma::format_delimited("PUBLISH" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_channel_name, n_message);
}

}
}
