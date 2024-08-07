#include "http_file_cache.hpp"

namespace duckdb {

CachedFile::CachedFile(const string &cache_dir, FileSystem &fs, const std::string &key, bool cache_file) : fs(fs) {
	file_name = cache_dir + "/" + key;

	GetDirectoryCacheLock(cache_dir);

	FileOpenFlags flags =
	    FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS | FileLockType::READ_LOCK;
	handle = fs.OpenFile(file_name, flags);
	if (handle) {
		initialized = true;
		size = handle->GetFileSize();
	} else if (cache_file) {
		flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE | FileLockType::WRITE_LOCK;
		handle = fs.OpenFile(file_name, flags);
	}

	ReleaseDirectoryCacheLock();
}

CachedFile::~CachedFile() {
	if (!initialized && handle) {
		fs.RemoveFile(file_name);
	}
}

void CachedFile::GetDirectoryCacheLock(const string &cache_dir) {
	std::string lock_file = cache_dir + "/.lock";
	FileOpenFlags flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                      FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE | FileFlags::FILE_FLAGS_NULL_IF_EXISTS |
	                      FileLockType::WRITE_LOCK;
	directory_lock_handle = fs.OpenFile(lock_file, flags);
	if (directory_lock_handle == nullptr) {
		flags = FileFlags::FILE_FLAGS_WRITE | FileLockType::WRITE_LOCK;
		directory_lock_handle = fs.OpenFile(lock_file, flags);
	}
}


void CachedFile::ReleaseDirectoryCacheLock() {
	directory_lock_handle->Close();
	directory_lock_handle.reset();
}


CachedFileHandle::CachedFileHandle(shared_ptr<CachedFile> &file_p) {
	// If the file was not yet initialized, we need to grab a lock.
	if (!file_p->initialized) {
		lock = make_uniq<lock_guard<mutex>>(file_p->lock);
	}
	file = file_p;
}

void CachedFileHandle::SetInitialized(idx_t total_size) {
	if (file->initialized) {
		throw InternalException("Cannot set initialized on cached file that was already initialized");
	}
	if (!lock) {
		throw InternalException("Cannot set initialized on cached file without lock");
	}
	file->size = total_size;
	file->initialized = true;
	file->handle->Close();
	lock = nullptr;

	FileOpenFlags flags = FileFlags::FILE_FLAGS_READ | FileLockType::READ_LOCK;
	file->handle = file->fs.OpenFile(file->file_name, flags);
}

void CachedFileHandle::Allocate(idx_t size) {
	if (file->initialized) {
		throw InternalException("Cannot allocate cached file that was already initialized");
	}
	file->handle->Trim(0, size);
	file->capacity = size;
}

void CachedFileHandle::GrowFile(idx_t new_capacity, idx_t bytes_to_copy) {
	file->handle->Trim(bytes_to_copy, new_capacity);
}

void CachedFileHandle::Write(const char *buffer, idx_t length, idx_t offset) {
	//! Only write to non-initialized files with a lock;
	D_ASSERT(!file->initialized && lock);
	file->handle->Write((void *)buffer, length, offset);
}

void CachedFileHandle::Read(void *buffer, idx_t length, idx_t offset) {
	//! Only read to initialized files without a lock;
	D_ASSERT(file->initialized && !lock);
	file->handle->Read((void *)buffer, length, offset);
}

//! Get cache entry, create if not exists only if caching is enabled
shared_ptr<CachedFile> HTTPFileCache::GetCachedFile(const string &cache_dir, const string &key, bool cache_file) {
	lock_guard<mutex> lock(cached_files_mutex);
	auto it = cached_files.find(key);
	if (it != cached_files.end()) {
		return it->second;
	}
	auto cache_entry = make_shared_ptr<CachedFile>(cache_dir, db->GetFileSystem(), key, cache_file);
	if (cache_entry->Initialized() || cache_file) {
		cached_files[key] = cache_entry;
		return cache_entry;
	} else {
		return nullptr;
	}
}

} // namespace duckdb
