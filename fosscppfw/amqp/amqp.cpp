#include "amqp.h"

#include <algorithm>
#include <amqpcpp.h>
#include <memory>
#include <stdexcept>
#include <zstd.h>

namespace AMQP {

ReplyCompression parseReplyCompression(std::string_view value) {
	if (value == "none") return ReplyCompression::None;
	if (value == "zstd") return ReplyCompression::Zstd;
	throw std::invalid_argument("Reply compression must be 'none' or 'zstd'");
}

ReplyCompression negotiateReplyCompression(ReplyCompression configured, Table const& headers) {
	auto const& acceptedEncoding = headers.get("x-accept-reply-encoding");
	return acceptedEncoding.isString() && static_cast<std::string const&>(acceptedEncoding) == "zstd"
		? configured : ReplyCompression::None;
}

std::optional<std::string> compressReply(std::string_view payload, ReplyCompression compression) {
	constexpr size_t maxReplyBytes = 64 * 1024 * 1024;
	if (compression != ReplyCompression::Zstd || payload.size() < 2 || payload.size() > maxReplyBytes) {
		return std::nullopt;
	}
	try {
		std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(), ZSTD_freeCCtx);
		if (!context
			|| ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_compressionLevel, 1))
			|| ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, 1))
			|| ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, 1))) {
			return std::nullopt;
		}
		// A smaller bounded destination makes incompressible replies fall back before publication.
		std::string compressed(payload.size() - 1, '\0');
		size_t const bytes = ZSTD_compress2(
			context.get(), compressed.data(), compressed.size(), payload.data(), payload.size()
		);
		if (ZSTD_isError(bytes)) return std::nullopt;
		compressed.resize(bytes);
		return compressed;
	} catch (std::bad_alloc const&) {
		// Compression is optional; the already-produced plain reply remains usable.
		return std::nullopt;
	}
}

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

std::optional<PreparedReply> prepareReply(
	std::string const& payload, Table const& requestHeaders, ReplyCompression compression,
	ReplyPreparer const& preparer, std::string const& correlationId, size_t maxFrameBytes
) {
	std::optional<PreparedReply> prepared;
	if (preparer) {
		prepared = preparer(payload, requestHeaders, compression);
	} else if (auto compressed = compressReply(payload, negotiateReplyCompression(compression, requestHeaders))) {
		prepared = PreparedReply {std::move(*compressed), {}, "zstd"};
	}
	if (!prepared || prepared->headers.empty()) return prepared;
	Table headers;
	for (auto const& entry : prepared->headers) headers[entry.first] = entry.second;
	MetaData metadata;
	metadata.setHeaders(headers);
	metadata.setContentEncoding(prepared->contentEncoding);
	metadata.setCorrelationID(correlationId);
	metadata.setTypeName("multipart/incomplete");
	// AMQP content-header framing: frame prefix/end (8), class/weight (4), body size (8).
	if (maxFrameBytes != 0 && 20u + metadata.size() > maxFrameBytes) return std::nullopt;
	return prepared;
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
