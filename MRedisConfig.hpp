//  Copyright 2018 Stephan Menzel. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// see https://gcc.gnu.org/wiki/Visibility

// Generic helper definitions for shared library support
#if defined _WIN32 || defined __CYGWIN__
	#define MREDIS_DLL_IMPORT __declspec(dllimport)
	#define MREDIS_DLL_EXPORT __declspec(dllexport)
	#define MREDIS_DLL_LOCAL

	#ifndef NOMINMAX
		#define NOMINMAX
	#endif
#else
	#if __GNUC__ >= 4
		#define MREDIS_DLL_IMPORT __attribute__ ((visibility ("default")))
		#define MREDIS_DLL_EXPORT __attribute__ ((visibility ("default")))
		#define MREDIS_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
	#else
		#define MREDIS_DLL_IMPORT
		#define MREDIS_DLL_EXPORT
		#define MREDIS_DLL_LOCAL
	#endif
#endif

#ifdef MREDIS_DLL // defined if is compiled as a DLL
	#ifdef mredis_EXPORTS // defined if we are building the DLL (instead of using it)
		#define MREDIS_API MREDIS_DLL_EXPORT
	#else
		#define MREDIS_API MREDIS_DLL_IMPORT
	#endif // mredis_EXPORTS
	#define MREDIS_LOCAL MREDIS_DLL_LOCAL
#else // MREDIS_DLL is not defined: this means it is a static lib.
	#define MREDIS_API
	#define MREDIS_LOCAL
#endif // MREDIS_DLL



#ifdef _MSC_VER
#pragma warning (disable : 4996) // Function call with parameters that may be unsafe.
#endif
