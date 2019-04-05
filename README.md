
# MRedis

This is a C++ asynchronous client library for the [redis](http://redis.io) 
memory cache system. It depends almost entirely on [Boost](http://www.boost.org)
and is taylored towards extensibility and convenience of use. Target platforms 
are Windows and GNU/Linux systems.

This is work in progress and far from complete.

There are [many such clients](https://redis.io/clients#c--), most of 
which are certainly better than this. So why does the world need another? 

Reasons for using this are:
 * Reference library 'hiredis' did not build on Windows and doesn't officially support it. Many other libraries are wrappers around hiredis so they did not work for me as a consequence.
 * Other libraries had trouble building on Windows or tried to build their own hiredis in place.
 * You already use CMake and depend on Boost and do not want further dependencies.
 * You want to contribute
  
## Dependencies

 * [Boost](http://www.boost.org) (>= 1.67.0)
 * [CMake](http://www.cmake.org) (>= 3.10)
 * [Moose Tools](https://github.com/MrMoose/moose_tools)

## Installation

This has no installation process yet. It is meant to be included into 
any CMake based project as a submodule and built in place along with your code.
A second submodule - tools - also needs to be included and named `tools`

```
 $> cd your/project/source
 $> git submodule add https://github.com/MrMoose/moose_tools.git tools
 $> git submodule add https://github.com/MrMoose/mredis.git mredis
 $> git submodule update --init
```
Your dependency on Boost should already be established at that point.
MRedis is assuming that...

```
find_package(Boost 1.67.0 REQUIRED COMPONENTS chrono thread system program_options log unit_test_framework)
```

... will yield the required CMake targets.
Then you should add those directories into your CMakelists.txt.

```
add_subdirectory(tools)
add_subdirectory(mredis)
```

Next time you run CMake, mredis will its own target and a test tool. To use it, add `mredis` to
your target's dependencies like this:

```
add_executable(my_executable my_executable.cpp)
target_link_libraries(my_executable
	mredis
)
```

## Usage

Main access class is `AsyncClient`. It has all redis commands (that I bothered 
to implement yet) exposed as async functions. Most functions come in two flavors:
 * One returning a future result which will be set when the client completes your command
 * An extra overload which accepts a callback lambda that will be executed when your result is ready
 
### Futures

Using the future overloads is convenient and simple.

```
#include "mredis/AsyncClient.hpp"

using namespace moose::mredis;

// Name your server's IP in the constructor.
AsyncClient client("127.0.0.1");

 // synchronous connect blocks until ready
client.connect();

// set some value. We don't need a response here.
client.set("answer", "42");   

// read the value back in and get a future for when it's there.
future_response fr = client.get("answer");

// blocks until response is available 
RedisMessage msg = fr.get();

if (is_string(msg)) {
	std::cout << "The answer is " << boost::get<std::string>(msg) << std::endl;
} else {
	std::cerr << "Wrong response type" << std::endl;
}

```

### Retrievers

There are two variantions of so-called Retrievers, which are helper structs 
that ease usage of the futures by providing type checks of the returned values,
as well as a timeout. They also encapsulate this behavior for use in a multi-threaded
scenario or a fiber scenario.

When unsure which usage is best for you, this would be the one.

```
BlockingRetriever< std::string > value_getter{ 2 };
client.get("test_value", value_getter.responder());
const boost::optional<std::string> time_values = value_getter.wait_for_response();
```

This will block this thread for 2 seconds until the value is retrieved and 
checks the return for being a string. The optional will not be set if the value
is not present in redis.
A timeout of two seconds will cause a `redis_error` exception.

Likewise, you can do the same in a fiber, without blocking all of them:

```
FiberRetriever< std::string > value_getter{ 2 };
client.get("test_value", value_getter.responder());
const boost::optional<std::string> time_values = value_getter.wait_for_response();
```

Behavior is the same.


### Callbacks

Giving in lambdas can be more elaborate and complex.

```
#include "mredis/AsyncClient.hpp"

using namespace moose::mredis;

// Name your server's IP in the constructor.
AsyncClient client("127.0.0.1");

 // synchronous connect blocks until ready
client.connect();

// set some value. We don't need a response here.
client.set("answer", "42");   

// read the value back in and execute a lambda when it's there.
future_response fr = client.get("answer", [] (const RedisMessage &n_response) {

	if (is_string(msg)) {
		std::cout << "The answer is " << boost::get<std::string>(msg) << std::endl;
	} else {
		std::cerr << "Wrong response type" << std::endl;
	}
});
```


## License

Boost Software License - Version 1.0 - August 17th, 2003

Permission is hereby granted, free of charge, to any person or organization
obtaining a copy of the software and accompanying documentation covered by
this license (the "Software") to use, reproduce, display, distribute,
execute, and transmit the Software, and to prepare derivative works of the
Software, and to permit third-parties to whom the Software is furnished to
do so, all subject to the following:

The copyright notices in the Software and this entire statement, including
the above license grant, this restriction and the following disclaimer,
must be included in all copies of the Software, in whole or in part, and
all derivative works of the Software, unless such copies or derivative
works are solely in the form of machine-executable object code generated by
a source language processor.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

