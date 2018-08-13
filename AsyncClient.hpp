
//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once
#include "MRedisConfig.hpp"
#include "MRedisResult.hpp"

#include <boost/thread/mutex.hpp>
#include <boost/cstdint.hpp>
#include <boost/asio/io_context.hpp>

namespace moose {
namespace mredis {

class MRedisTCPConnection;

/*! @brief simple async Redis client.
	And yes, I know. Classes that are described as "simple" generally turn out 
	not at all simple or broken or both. But I have no choice. None of the available 
	redis client libs are suitable for this.

	@note This is meant to use an io_context that is already run elsewhere.
		It relies on the fact that this actually is run. Also, it does not support 
		multiple threads running that io_context. Implicit strand.
 */
class AsyncClient {

	public:
		
		//! use local unix domain socket
		MREDIS_API AsyncClient(boost::asio::io_context &n_io_context);

		/*! use IP to do TCP connect
			@note asserts on empty server
		 */
		MREDIS_API AsyncClient(boost::asio::io_context &n_io_context, const std::string &n_server, const boost::uint16_t n_port = 6379);

		MREDIS_API virtual ~AsyncClient() noexcept;

		/*! @brief sync connect and block until connected
			@note if already connected, will re-connect
			@throw network_error on cannot resolve host name
		 */
		MREDIS_API void connect();

		/*! @brief start async connect, return immediately
			Once connected, returned future will turn true for OK or false for not OK
			@note if already connected, will re-connect
			@throw network_error on cannot resolve host name
		 */
		MREDIS_API boost::shared_future<bool> async_connect();

		/*! @defgroup basic functions
			They all assert when connect wasn't called.
			@{
		*/

		/*! @brief most basic get
			@param n_key assert on empty
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/get
		 */
		MREDIS_API void get(const std::string &n_key, Callback &&n_callback) noexcept;

		/*! @brief most basic get
			@param n_key assert on empty
			@returns future which will hold response, may also hold exception
			@see https://redis.io/commands/get
		 */
		MREDIS_API future_response get(const std::string &n_key) noexcept;

		/*! @brief most basic set
			@param n_key assert on empty
			@param n_value
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/set
		 */
		MREDIS_API void set(const std::string &n_key, const std::string &n_value, Callback &&n_callback) noexcept;

		/*! @brief most basic set
			@param n_key assert on empty
			@param n_value
			@returns future which will hold response, may also hold exception
			@see https://redis.io/commands/set
		*/
		MREDIS_API future_response set(const std::string &n_key, const std::string &n_value) noexcept;

		/*! @brief field increment by 1
			@param n_key assert on empty
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/incr
		 */
		MREDIS_API void incr(const std::string &n_key, Callback &&n_callback) noexcept;

		/*! @brief field increment by 1
			@param n_key assert on empty
			@returns future which will hold response, may also hold exception
			@see https://redis.io/commands/incr
		*/
		MREDIS_API future_response incr(const std::string &n_key) noexcept;


		/*! @} */

		/*! @defgroup hash map functions
			They too assert when connect wasn't called.
			@{
		*/

		/*! @brief hash map field increment
			@param n_hash_name assert on empty
			@param n_field_name assumed to be an integer field
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/hincrby
		 */
		MREDIS_API void hincrby(const std::string &n_hash_name,
		                        const std::string &n_field_name,
		                        const boost::int64_t n_increment_by,
		                        Callback &&n_callback) noexcept;

		/*! @brief hash map field increment
			@param n_hash_name assert on empty
			@param n_field_name assumed to be an integer field
			@returns future which will hold response, may also hold exception
			@see https://redis.io/commands/hincrby
		*/
		MREDIS_API future_response hincrby(const std::string &n_hash_name,
		                        const std::string &n_field_name,
		                        const boost::int64_t n_increment_by) noexcept;

		/*! @brief hash map field get
			@param n_hash_name assert on empty
			@param n_field_name 
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/hget
		*/
		MREDIS_API void hget(const std::string &n_hash_name,
		                        const std::string &n_field_name,
		                        Callback &&n_callback) noexcept;
	
		/*! @brief hash map field get
			@param n_hash_name assert on empty
			@param n_field_name
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/hget
		 */
		MREDIS_API future_response hget(const std::string &n_hash_name,
			                    const std::string &n_field_name) noexcept;
	
		/*! @brief hash map field setter
			@param n_hash_name assert on empty
			@param n_field_name assert on empty
			@param n_value whatever you want to set as value
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/hset
		 */
		MREDIS_API void hset(const std::string &n_hash_name,
		                        const std::string &n_field_name,
		                        const std::string &n_value,
		                        Callback &&n_callback) noexcept;

		/*! @brief hash map field setter
			@param n_hash_name assert on empty
			@param n_field_nameassert on empty
			@param n_value whatever you want to set as value
			@param n_callback must be no-throw, will not be executed in caller's thread
			@see https://redis.io/commands/hset
		 */
		MREDIS_API future_response hset(const std::string &n_hash_name,
		                        const std::string &n_field_name,
		                        const std::string &n_value) noexcept;
		/*! @} */


		/*! @defgroup unordered set functions
			They too assert when connect wasn't called.
			@{
		*/

		/*! @brief set add
			@param n_set_name the name of your set
			@param n_value what to insert
			@param n_callback must be no-throw
			@see https://redis.io/commands/sadd
		*/
		MREDIS_API void sadd(const std::string &n_set_name,
		                        const std::string &n_value,
		                        Callback &&n_callback) noexcept;

		/*! @brief set add
			@param n_set_name the name of your set
			@param n_value what to insert
			
			@returns future int with number of items added, may also hold exception
			@see https://redis.io/commands/sadd
		*/
		MREDIS_API future_response sadd(const std::string &n_set_name, const std::string &n_value) noexcept;


		/*! @} */

	private:

		friend class MRedisTCPConnection;

		boost::asio::io_context             &m_io_context;
		const std::string                    m_server;         //!< if tcp, is set to server
		const boost::uint16_t                m_port;
		std::unique_ptr<MRedisTCPConnection> m_connection;
};

}
}

