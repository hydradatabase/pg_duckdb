#pragma once

#include <mutex>

namespace pgduckdb {

/*
 * GlobalProcessLock is used to synchronize calls to PG functions that modify global variables. Examples
 * for this synchronization are functions that read buffers/etc. This lock is shared between all threads and all
 * replacement scans.
 */
struct GlobalProcessLock {
public:
	static std::mutex &
	GetLock() {
		static std::mutex lock;
		return lock;
	}
};

} // namespace pgduckdb
