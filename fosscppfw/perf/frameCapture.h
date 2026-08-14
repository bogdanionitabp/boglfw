/*
 * frameCapture.h
 *
 *  Created on: Dec 17, 2016
 *	  Author: bog
 */

#ifndef PERF_FRAMECAPTURE_H_
#define PERF_FRAMECAPTURE_H_

#ifdef ENABLE_PERF_PROFILING

#include "callGraph.h"

#include "../utils/MTVector.h"

#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <cstring>
#include <atomic>
#include <memory>

namespace perf {

class FrameCapture {
public:
	enum CaptureMode {
		Disabled,
		ThisThreadOnly,
		AllThreads,
	};

	struct frameData {
		std::chrono::time_point<std::chrono::high_resolution_clock> startTime_;
		std::chrono::time_point<std::chrono::high_resolution_clock> endTime_;
		char name_[256];
		unsigned threadIndex_;
		bool deadTime_;
		bool idleTime_;

		frameData(const char name[], std::chrono::time_point<std::chrono::high_resolution_clock> start,
				unsigned threadIndex, bool deadTime, bool idleTime)
			: startTime_(start), threadIndex_(threadIndex), deadTime_(deadTime), idleTime_(idleTime) {
			strncpy(name_, name, sizeof(name_)/sizeof(name_[0]) - 1);
		}
	};

	// start capturing a frame, recording all markers' absolute times
	static void start(CaptureMode mode);
	// stop capturing the frame
	static void stop();

	// aggregates all calls from all recorded threads (depending on capture mode) and returns a linear sequence
	// ordered chronologically, with interleaved threads
	static std::vector<frameData> getResults();

	// returns the name of the thread identified by index (use index from frameData)
	static std::string getThreadNameForIndex(unsigned index);

	// call this to clean up all data and start fresh next time
	static void cleanup();

private:
	friend class Marker;

	static bool captureEnabledOnThisThread() {
		auto mode = mode_.load(std::memory_order_acquire);
		if (mode == Disabled)
			return false;
		return (mode == AllThreads || std::this_thread::get_id() == exclusiveThreadID_.load(std::memory_order_consume));
	}

	static void beginFrame(const char name[], std::chrono::time_point<std::chrono::high_resolution_clock> now, bool deadTime, bool idleTime) {
		auto &ti = getThreadInstance();
		ti.frames_->emplace_back(name, now, ti.threadIndex_, deadTime, idleTime);
		ti.frameStack_.push(ti.frames_->size()-1);
	}

	static void endFrame(std::chrono::time_point<std::chrono::high_resolution_clock> now) {
		auto &ti = getThreadInstance();
		if (!ti.frameStack_.size()) {
			beginFrame("{UNKNOWN}", captureStartTime_, true, false);
		}
		size_t idx = ti.frameStack_.top();
		ti.frameStack_.pop();
		// cleanup() on another thread may have cleared frames_; skip stale indices
		if (idx < ti.frames_->size()) {
			(*ti.frames_)[idx].endTime_ = now;
		}
	}

	static std::atomic<CaptureMode> mode_;
	static std::atomic<std::thread::id> exclusiveThreadID_;
	static MTVector<std::shared_ptr<std::vector<frameData>>> allFrames_;
	static MTVector<std::string> threadNames_;
	static std::chrono::time_point<std::chrono::high_resolution_clock> captureStartTime_;

	std::shared_ptr<std::vector<frameData>> frames_;
	std::stack<int> frameStack_;
	unsigned threadIndex_;
	static FrameCapture& getThreadInstance() {
		static thread_local FrameCapture instance;
		return instance;
	}
	FrameCapture() : frames_(new std::vector<frameData>()) {
		frames_->reserve(1024);
		allFrames_.push_back(frames_);
		threadIndex_ = threadNames_.push_back(CallGraph::getCrtThreadName());
	}
};

} /* namespace perf */

#endif // ENABLE_PERF_PROFILING

#endif /* PERF_FRAMECAPTURE_H_ */
