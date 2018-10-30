
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "RESP.hpp"

#include "tools/Assert.hpp"
// 
// #define BOOST_SPIRIT_DEBUG
// #define BOOST_SPIRIT_DEBUG_PRINT_SOME 200
// #define BOOST_SPIRIT_DEBUG_OUT std::cerr

#include <boost/asio.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>
#include <boost/spirit/include/qi_parse.hpp>
#include <boost/spirit/include/karma_format.hpp>
#include <boost/spirit/include/support_multi_pass.hpp>
#include <boost/spirit/include/phoenix.hpp>

namespace moose {
namespace mredis {

namespace qi = boost::spirit::qi;
namespace ascii = qi::ascii;
namespace karma = boost::spirit::karma;
namespace phx = boost::phoenix;

template <typename InputIterator>
struct simple_string_parser : qi::grammar<InputIterator, std::string()> {

	simple_string_parser() : simple_string_parser::base_type(m_simple_start, "simple_string") {

		// The docs are not very specific on what a simple ascii char is. I assume it's char_ except for space and newlines
		m_simple_start %= '+' >> +(ascii::char_ - ascii::char_("\r\n")) >> "\r\n";

//		BOOST_SPIRIT_DEBUG_NODES((m_simple_start));
	}

	qi::rule<InputIterator, std::string()>  m_simple_start;
};

template <typename InputIterator>
struct bulk_string_parser : qi::grammar<InputIterator, std::string()> {

	bulk_string_parser()
			: bulk_string_parser::base_type(m_bulk_start, "bulk_string")
			, m_size(0) {

		using qi::labels::_1;
		using qi::_val;

		m_prefix      = '$' >> qi::ulong_[phx::ref(m_size) = _1] >> "\r\n";
		m_bulk_start %= m_prefix >> qi::repeat(phx::ref(m_size))[qi::char_] >> "\r\n";

//		BOOST_SPIRIT_DEBUG_NODES((m_prefix)(m_bulk_start));
	}

	std::size_t                             m_size;
	qi::rule<InputIterator>                 m_prefix;
	qi::rule<InputIterator, std::string()>  m_bulk_start;
};

template <typename InputIterator>
struct null_parser : qi::grammar<InputIterator, null_result()> {

	null_parser() : null_parser::base_type(m_null_start, "null") {

		using qi::labels::_1;

		m_null_start = qi::lit("$-1\r\n")[_1 = phx::construct<null_result>()];
	}

	qi::rule<InputIterator, null_result()>  m_null_start;
};

template <typename InputIterator>
struct integer_parser : qi::grammar<InputIterator, boost::int64_t()> {

	integer_parser() : integer_parser::base_type(m_int_start, "integer") {

		m_int_start %= ':' >> qi::long_long >> qi::eol;
//		BOOST_SPIRIT_DEBUG_NODE(m_int_start);
	}

	qi::rule<InputIterator, boost::int64_t()>  m_int_start;
};

template <typename InputIterator>
struct array_parser : qi::grammar<InputIterator, std::vector<RESPonse> > {

	array_parser() 
			: array_parser::base_type(m_start, "array")
			, m_size(0) {

		m_prefix = '*' >> qi::ulong_[phx::ref(m_size) = qi::labels::_1] >> "\r\n";

		// I assume arrays can not contain error or further arrays. Not quite sure about the errors though
		m_variant %= m_simple_string | m_integer | m_null_result | m_bulk_string;

		m_start %= m_prefix >> qi::repeat(phx::ref(m_size))[m_variant];
	}

	std::size_t                                      m_size;
	qi::rule<InputIterator>                          m_prefix;
	qi::rule<InputIterator, RESPonse()>              m_variant;
	integer_parser<InputIterator>                    m_integer;
	simple_string_parser<InputIterator>              m_simple_string;
	bulk_string_parser<InputIterator>                m_bulk_string;
	null_parser<InputIterator>                       m_null_result;
	qi::rule<InputIterator, std::vector<RESPonse> >  m_start;
};

template <typename InputIterator>
struct error_parser : qi::grammar<InputIterator, redis_error()> {

	error_parser() : error_parser::base_type(m_start, "error") {

		using qi::_val;
		using qi::labels::_1;

		// Very similar to basic string but I need a different type		
		m_error_text %= +(ascii::char_ - qi::eol);
		
		m_start = qi::lit('-')[ _val = phx::construct<redis_error>() ] >> 
				m_error_text[ phx::bind(&redis_error::set_server_message, _val, _1) ] >> qi::eol;
	}

	qi::rule<InputIterator, std::string()>  m_error_text;
	qi::rule<InputIterator, redis_error()>  m_start;
};

template <typename InputIterator>
struct response_parser : qi::grammar<InputIterator, RESPonse()> {

	response_parser() : response_parser::base_type(m_start, "response") {
	
		m_start %= m_simple_string | m_integer | m_array | m_null_result | m_bulk_string | m_error;
	}

	integer_parser<InputIterator>        m_integer;
	simple_string_parser<InputIterator>  m_simple_string;
	bulk_string_parser<InputIterator>    m_bulk_string;
	null_parser<InputIterator>           m_null_result;
	array_parser<InputIterator>          m_array;
	error_parser<InputIterator>          m_error;
	qi::rule<InputIterator, RESPonse()>  m_start;
};

using karma::no_delimit;
using karma::repeat;
using karma::uint_;
using karma::byte_;
using karma::string;
using karma::lit;

bool parse(const std::string &n_input, RESPonse &n_response) {

	std::string::const_iterator first = n_input.cbegin();
	const std::string::const_iterator last = n_input.cend();
	response_parser<std::string::const_iterator> p;
	return qi::parse(first, last, p, n_response);
}

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
		karma::generate(out, uint_, std::chrono::duration_cast<std::chrono::seconds>(n_expire_time).count());
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

void format_incr(std::ostream &n_os, const std::string &n_key) {
	
	n_os << karma::format_delimited("INCR" << karma::string << karma::no_delimit["\r\n"], " ", n_key);
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

void format_eval(std::ostream &n_os, const std::string &n_script, const std::vector<LuaArgument> &n_args) {

	// This is Lua script eval. I am assuming the script doesn't contain anything weird (null bytes)
	// It does however contain lots of newlines.
	// The keys and values however might contain binary. So I try the bulk approach first

	// According to how I read the protocol specs Redis should accept arrays of mixed types
	// However whenever I do it I get a protocol error. So I convert the number of arguments
	// to a string first. This should not be necessary

	std::string argstr;
	std::back_insert_iterator<std::string> out(argstr);
	karma::generate(out, uint_, n_args.size());

	const std::size_t num_fields = 3    // EVAL $SCRIPT $NUMBER_OF_ARGUMENTS ...
			+ n_args.size() * 2;         // each argument as one key and one value

	n_os << karma::format_delimited(
		no_delimit['*'] << uint_ << // Array of how many fields...
		lit("$4") <<                // Bulk string of length 4 (length of the term "EVAL")
		lit("EVAL") <<              // eval command
		no_delimit['$'] << uint_ << // binary length of script
		string <<                   // script
		no_delimit['$'] << uint_ << // length of number of arguments string
		string                      // number of arguments string
		, "\r\n", num_fields, n_script.size(), n_script, argstr.size(), argstr);

	// First all the keys
	for (const LuaArgument &a: n_args) {
		n_os << karma::format_delimited(
			no_delimit['$'] << uint_ << // binary length of script
			string                      // script
			, "\r\n", a.key().size(), a.key());
	}

	// Now again for all the values
	for (const LuaArgument &a : n_args) {
		n_os << karma::format_delimited(
			no_delimit['$'] << uint_ << // binary length of script
			string                      // script
			, "\r\n", a.value().size(), a.value());
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

RESPonse parse_one(std::istream &n_is) {
	
	typedef std::istreambuf_iterator<char> base_iterator_type;
	boost::spirit::multi_pass<base_iterator_type> first =
		boost::spirit::make_default_multi_pass(base_iterator_type(n_is));
	boost::spirit::multi_pass<base_iterator_type> last =
		boost::spirit::make_default_multi_pass(base_iterator_type());
	
	response_parser<boost::spirit::multi_pass<base_iterator_type> > p;

	RESPonse result;
	if (!qi::parse(first, last, p, result) || (first != last)) {
		return redis_error();
	} else {
		return result;
	}
}

// since we should only be parsing from one thread,
// it should be safe to re-use one object
typedef std::istreambuf_iterator<char> stream_iterator_type;
response_parser<boost::spirit::multi_pass<stream_iterator_type> > s_response_parser;

bool parse_from_stream(std::istream &n_is, RESPonse &n_response) noexcept {

	typedef std::istreambuf_iterator<char> base_iterator_type;
	boost::spirit::multi_pass<base_iterator_type> first =
		boost::spirit::make_default_multi_pass(base_iterator_type(n_is));
	boost::spirit::multi_pass<base_iterator_type> last =
		boost::spirit::make_default_multi_pass(base_iterator_type());

//	response_parser<boost::spirit::multi_pass<base_iterator_type> > p;

	return qi::parse(first, last, s_response_parser, n_response);
}

}
}
