
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "RESP.hpp"

#include "tools/Assert.hpp"

#include <boost/asio.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>
#include <boost/spirit/include/qi_parse.hpp>
#include <boost/spirit/include/karma_format.hpp>
#include <boost/spirit/include/support_multi_pass.hpp>

namespace moose {
namespace mredis {

namespace qi = boost::spirit::qi;
namespace ascii = qi::ascii;
namespace karma = boost::spirit::karma;

template <typename InputIterator>
struct simple_string_parser : qi::grammar<InputIterator, std::string()> {

	simple_string_parser() : simple_string_parser::base_type(m_start, "simple_string") {

		// The docs are not very specific on what a simple ascii char is. I assume it's char_ except for space and newlines
		m_start %= '+' >> +(ascii::char_ - qi::eol) >> qi::eol;
	}

	qi::rule<InputIterator, std::string()>  m_start;
};

template <typename InputIterator>
struct integer_parser : qi::grammar<InputIterator, boost::int64_t()> {

	integer_parser() : integer_parser::base_type(m_start, "integer") {

		m_start %= ':' >> qi::long_long >> qi::eol;
	}

	qi::rule<InputIterator, boost::int64_t()>  m_start;
};

template <typename InputIterator>
struct response_parser : qi::grammar<InputIterator, RESPonse()> {

	response_parser() : response_parser::base_type(m_start, "response") {
	
		m_start %= m_simple_string | m_integer;
	}

	integer_parser<InputIterator>        m_integer;
	simple_string_parser<InputIterator>  m_simple_string;
	qi::rule<InputIterator, RESPonse()>  m_start;
};

RESPonse parse(const std::string n_input) {

	return 0;
}

void format_ping(std::ostream &n_os) {

	n_os << karma::format("PING\r\n");
}

void format_hincrby(std::ostream &n_os, const std::string &n_hash_name, const std::string &n_field_name) {

	n_os << karma::format("HINCRBY " << karma::string << " " << karma::string << "\r\n", n_hash_name, n_field_name);
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

}
}