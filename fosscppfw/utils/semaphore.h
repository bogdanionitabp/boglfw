#pragma once
#include <mutex>
#include <condition_variable>
#ifdef __APPLE__
#include <pthread.h>
#endif

class Semaphore {
private:
	std::mutex mutex_;
	std::condition_variable condition_;
	unsigned long count_ = 0; // Initialized as locked.

public:
	Semaphore(unsigned long count = 0):
		count_(count) {}

	void notify() {
		std::lock_guard<decltype(mutex_)> lock(mutex_);
		++count_;
		condition_.notify_one();
	}

	void wait() {
		std::unique_lock<decltype(mutex_)> lock(mutex_);
#ifdef __APPLE__
		// Darwin does not unwind unique_lock when pthread_cancel interrupts this wait.
		pthread_cleanup_push([](void* guard) { static_cast<decltype(lock)*>(guard)->unlock(); }, &lock);
#endif
		while(!count_) // Handle spurious wake-ups.
			condition_.wait(lock);
		--count_;
#ifdef __APPLE__
		pthread_cleanup_pop(0);
#endif
	}

	bool try_wait() {
		std::lock_guard<decltype(mutex_)> lock(mutex_);
		if(count_) {
			--count_;
			return true;
		}
		return false;
	}
};
