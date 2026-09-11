#include "amqp.h"

#include <algorithm>

namespace AMQP {

size_t getReplyChunkSize(std::string_view remainingPayload, size_t maxChunkBytes, bool preserveUtf8Boundaries) {
	size_t const chunkSize = std::min(remainingPayload.size(), maxChunkBytes);
	if (!preserveUtf8Boundaries || chunkSize == remainingPayload.size() || chunkSize == 0) {
		return chunkSize;
	}
	auto isContinuation = [&remainingPayload](size_t offset) {
		return (static_cast<unsigned char>(remainingPayload[offset]) & 0xc0) == 0x80;
	};
	size_t boundary = chunkSize;
	// A valid UTF-8 character has at most three continuation bytes.
	while (boundary > 0 && chunkSize - boundary < 3 && isContinuation(boundary)) {
		--boundary;
	}
	// Invalid byte sequences still make progress instead of producing an empty chunk.
	return boundary > 0 && !isContinuation(boundary) ? boundary : chunkSize;
}

std::vector<QueueConfig> generateXRandomQueues(std::string const& exchangeName, int count, MQHandler handler) {
	std::vector<QueueConfig> queues;
	if (count <= 0) {
		return {};
	}
	for (int i = 0; i < count; i++) {
		queues.push_back(
			QueueConfig(exchangeName + "-queue-" + std::to_string(i + 1))
				.setExchangeBinding(exchangeName, "")
				.setHandler(handler)
		);
	}
	return queues;
}

} // namespace AMQP
