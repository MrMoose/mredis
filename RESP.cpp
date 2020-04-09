
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "RESP.hpp"

// #define BOOST_SPIRIT_DEBUG
// #define BOOST_SPIRIT_DEBUG_PRINT_SOME 200
// #define BOOST_SPIRIT_DEBUG_OUT std::cerr

#include "tools/Log.hpp"

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
using namespace moose::tools;

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

unsigned long read_string_size(const std::string &v) {

	return static_cast<unsigned long>(v.size());
}

template <typename OutputIterator>
struct bulk_string_generator : karma::grammar<OutputIterator, std::string()> {

	bulk_string_generator() : bulk_string_generator::base_type(m_bulk_start, "bulk_string") {

		using karma::labels::_1;
		using karma::labels::_val;

		m_prefix    %= '$' << karma::ulong_ << "\r\n";
		m_bulk_start = m_prefix[_1 = phx::bind(&read_string_size, _val)] << karma::string[_1 = _val] << "\r\n";
	}

	karma::rule<OutputIterator, unsigned long()> m_prefix;
	karma::rule<OutputIterator, std::string()>   m_bulk_start;
};

template <typename InputIterator>
struct null_parser : qi::grammar<InputIterator, null_result()> {

	null_parser() : null_parser::base_type(m_null_start, "null") {

		using qi::labels::_1;

		m_null_start = qi::lit("$-1\r\n")[_1 = phx::construct<null_result>()];
	}

	qi::rule<InputIterator, null_result()>  m_null_start;
};

template <typename OutputIterator>
struct null_generator : karma::grammar<OutputIterator, null_result()> {

	null_generator() : null_generator::base_type(m_null_start, "null") {

		using karma::labels::_1;

		m_null_start = karma::lit("$-1\r\n")[_1];
	}

	karma::rule<OutputIterator, null_result()>  m_null_start;
};

template <typename InputIterator>
struct integer_parser : qi::grammar<InputIterator, boost::int64_t()> {

	integer_parser() : integer_parser::base_type(m_int_start, "integer") {

		m_int_start %= ':' >> qi::long_long >> qi::eol;
//		BOOST_SPIRIT_DEBUG_NODE(m_int_start);
	}

	qi::rule<InputIterator, boost::int64_t()>  m_int_start;
};

template <typename OutputIterator>
struct integer_generator : karma::grammar<OutputIterator, boost::int64_t()> {

	integer_generator() : integer_generator::base_type(m_int_start, "integer") {

		m_int_start %= ':' << karma::long_long << "\r\n";
	}

	karma::rule<OutputIterator, boost::int64_t()>  m_int_start;
};

template <typename InputIterator>
struct array_parser : qi::grammar<InputIterator, std::vector<RedisMessage>() > {

	array_parser() 
			: array_parser::base_type(m_start, "array")
			, m_size(0) {

		m_prefix = '*' >> qi::ulong_[phx::ref(m_size) = qi::labels::_1] >> "\r\n";

		// I assume arrays can not contain error or further arrays. Not quite sure about the errors though
		m_variant %= m_simple_string | m_integer | m_null_result | m_bulk_string;

		m_start %= m_prefix >> qi::repeat(phx::ref(m_size))[m_variant];
	}

	std::size_t                                           m_size;
	qi::rule<InputIterator>                               m_prefix;
	qi::rule<InputIterator, RedisMessage()>               m_variant;
	integer_parser<InputIterator>                         m_integer;
	simple_string_parser<InputIterator>                   m_simple_string;
	bulk_string_parser<InputIterator>                     m_bulk_string;
	null_parser<InputIterator>                            m_null_result;
	qi::rule<InputIterator, std::vector<RedisMessage>() > m_start;
};

template <typename InputIterator>
struct error_parser : qi::grammar<InputIterator, redis_error()> {

	error_parser() : error_parser::base_type(m_start, "error") {

		using qi::_val;
		using qi::labels::_1;

		// Very similar to basic string but I need a different type		
		m_error_text %= +(ascii::char_ - qi::eol);

		m_start = qi::lit('-')[_val = phx::construct<redis_error>()] >>
			m_error_text[phx::bind(&redis_error::set_server_message, _val, _1)] >> qi::eol;
	}

	qi::rule<InputIterator, std::string()>  m_error_text;
	qi::rule<InputIterator, redis_error()>  m_start;
};

// phoenix function to extract the server message.
// A workaround for C++17 and above as phx::bind on the getter function stopped working
struct get_srv_message {
	std::string operator()(const redis_error &e) const { return e.server_message(); }
};
static const phx::function<get_srv_message> s_srv_msg_extract;


template <typename OutputIterator>
struct error_generator : karma::grammar<OutputIterator, redis_error()> {

	error_generator() : error_generator::base_type(m_start, "error") {

		using karma::labels::_1;
		using karma::labels::_val;

	//	m_start = '-' << karma::string[_1 = phx::bind(&redis_error::server_message, _val)] << "\r\n";

		// C++17 note: I had to change to that phoenix function as the bind above wouldn't compile with C++17 or higher
		// for unknown reasons.
		m_start = '-' << karma::string[_1 = s_srv_msg_extract(_val)] << "\r\n";
	}

	karma::rule<OutputIterator, redis_error()> m_start;
};


unsigned long read_msg_vec_size(const std::vector<RedisMessage> &n_vector) {

	return static_cast<unsigned long>(n_vector.size());
}

template <typename OutputIterator>
struct array_generator : karma::grammar<OutputIterator, std::vector<RedisMessage>() > {
	
	array_generator() : array_generator::base_type(m_start, "array") {

		using karma::labels::_1;
		using karma::labels::_val;

		m_prefix  %= '*' << karma::ulong_ << "\r\n";
		m_variant %= m_bulk_string | m_integer | m_null_result | m_error;
		m_start    = m_prefix[_1 = phx::bind(&read_msg_vec_size, _val)] << (*m_variant)[_1 = _val];
	}

	karma::rule<OutputIterator, unsigned long()>             m_prefix;
	karma::rule<OutputIterator, RedisMessage()>              m_variant;
	integer_generator<OutputIterator>                        m_integer;
	bulk_string_generator<OutputIterator>                    m_bulk_string;
	null_generator<OutputIterator>                           m_null_result;
	error_generator<OutputIterator>                          m_error;
	karma::rule<OutputIterator, std::vector<RedisMessage>() >  m_start;
};

template <typename InputIterator>
struct message_parser : qi::grammar<InputIterator, RedisMessage()> {

	message_parser() : message_parser::base_type(m_start, "message") {
	
		m_start %= m_simple_string | m_integer | m_array | m_null_result | m_bulk_string | m_error;
	}

	integer_parser<InputIterator>        m_integer;
	simple_string_parser<InputIterator>  m_simple_string;
	bulk_string_parser<InputIterator>    m_bulk_string;
	null_parser<InputIterator>           m_null_result;
	array_parser<InputIterator>          m_array;
	error_parser<InputIterator>          m_error;
	qi::rule<InputIterator, RedisMessage()>  m_start;
};

template <typename OutputIterator>
struct message_generator : karma::grammar<OutputIterator, RedisMessage()> {

	message_generator() : message_generator::base_type(m_start, "message") {

		m_start   %= m_integer | m_array | m_null | m_bulk_string | m_error;
	}
	
	null_generator<OutputIterator>                 m_null;
	array_generator<OutputIterator>                m_array;
	integer_generator<OutputIterator>              m_integer;
	bulk_string_generator<OutputIterator>          m_bulk_string; //! can't differentiate between simple and bulk yet :-(
	error_generator<OutputIterator>                m_error;
	karma::rule<OutputIterator, RedisMessage()>    m_start;
};

// since we should only be parsing from one thread,
// it should be safe to re-use one object
using stream_iterator_type = std::istreambuf_iterator<char>;
message_parser<boost::spirit::multi_pass<stream_iterator_type> > s_response_stream_parser;

message_parser<const char *> s_response_parser;

RedisMessage parse_one(std::istream &n_is) {
	
	boost::spirit::multi_pass<stream_iterator_type> first =
		boost::spirit::make_default_multi_pass(stream_iterator_type(n_is));
	const boost::spirit::multi_pass<stream_iterator_type> last =
		boost::spirit::make_default_multi_pass(stream_iterator_type());
	
	message_parser<boost::spirit::multi_pass<stream_iterator_type> > p;

	RedisMessage result;
	if (!qi::parse(first, last, s_response_stream_parser, result) || (first != last)) {
		return redis_error();
	} else {
		return result;
	}
}

bool parse_from_stream(std::istream &n_is, RedisMessage &n_response) noexcept {

// Original code problematic as incomplete parse will consume parts of the stream.
//
// 	boost::spirit::multi_pass<stream_iterator_type> first =
// 		boost::spirit::make_default_multi_pass(stream_iterator_type(n_is));
// 	const boost::spirit::multi_pass<stream_iterator_type> last =
// 		boost::spirit::make_default_multi_pass(stream_iterator_type());
// 
// 	const bool retval = qi::parse(first, last, s_response_parser, n_response);
// 
// 	return retval;
// 
//  Until a solution is found I'm gonna have to copy the input buffer and parse from there.

	return false;
}

bool parse_from_streambuf(boost::asio::streambuf &n_streambuf, RedisMessage &n_response) noexcept {

	if (!n_streambuf.size()) {
		return false;
	}

	const char *original_begin = reinterpret_cast<const char *>(n_streambuf.data().data());
	const char *begin_ptr = original_begin;
	const char *end_ptr = begin_ptr + n_streambuf.size();

	const bool retval = qi::parse(begin_ptr, end_ptr, s_response_parser, n_response);

	if (retval) {
		const std::size_t bytes_to_consume = begin_ptr - original_begin;
		MOOSE_ASSERT(bytes_to_consume)

		// We have parsed one message successfully. This means, begin_ptr 
		// should now point to where the msg ended. We consume the buffer to remove it
		n_streambuf.consume(bytes_to_consume);
	}

	return retval;
}


void generate_to_stream(std::ostream &n_os, const RedisMessage &n_message) {

	message_generator<boost::spirit::ostream_iterator> g;
	n_os << karma::format(g, n_message);
}

}
}
