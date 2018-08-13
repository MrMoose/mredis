
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

	n_os << karma::format_delimited("GET" << karma::string << karma::no_delimit["\r\n"], " ", n_key);
}

/*! @todo

In order for set to be able to use binary data, use this:

*3<crlf>
$3<crlf>SET<crlf>
${binary_key_length}<crlf>{binary_key_data}<crlf>
${binary_data_length}<crlf>{binary_data}<crlf>


*/

void format_set(std::ostream &n_os, const std::string &n_key, const std::string &n_value) {

	n_os << karma::format_delimited("SET" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_key, n_value);
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

void format_sadd(std::ostream &n_os, const std::string &n_set_name, const std::string &n_value) {

	n_os << karma::format_delimited("SADD" << karma::string << karma::no_delimit['\"' << karma::string << "\"\r\n"],
			" ", n_set_name, n_value);
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
