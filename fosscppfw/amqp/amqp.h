#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <functional>
#include <cstddef>

namespace AMQP {

using MQResultCallback = std::function<void(std::string)>;
using MQHandler = std::function<void(std::string payload, MQResultCallback resultCallback)>;

namespace detail {
	template <class SUBCLASS>
	struct BaseConfig {
		std::string name = "";
		bool durable = true;
		bool autodelete = false;
		/** optionally specify a message TTL (in milliseconds). Ignored if <= 0 */
		int messageTtl = 0;

		virtual ~BaseConfig() = default;

		explicit BaseConfig(std::string name)
			: name(name)
		{}

		SUBCLASS& setName(std::string name) { this->name = name; return *dynamic_cast<SUBCLASS*>(this); }
		SUBCLASS& setDurable(bool durable) { this->durable = durable; return *dynamic_cast<SUBCLASS*>(this); }
		SUBCLASS& setAutodelete(bool autodelete) { this->autodelete = autodelete; return *dynamic_cast<SUBCLASS*>(this); }
		SUBCLASS& setMessageTtl(int messageTtl) { this->messageTtl = messageTtl; return *dynamic_cast<SUBCLASS*>(this); }

	protected:
		BaseConfig() = default;
	};
} // namespace detail

struct QueueConfig: public detail::BaseConfig<QueueConfig> {
	struct Binding {
		std::string exchange;
		std::string routingKey;
	} exchangeBinding;
	MQHandler handler = nullptr;
	size_t replyChunkBytes = 32768;
	bool preserveReplyUtf8Boundaries = false;

	QueueConfig() = default;

	explicit QueueConfig(std::string name)
		: BaseConfig(name)
	{}

	QueueConfig& setExchangeBinding(std::string exchange, std::string routingKey) {
		exchangeBinding.exchange = exchange;
		exchangeBinding.routingKey = routingKey;
		return *this;
	}

	QueueConfig& setHandler(MQHandler handler) {
		this->handler = handler;
		return *this;
	}

	QueueConfig& setReplyChunkBytes(size_t bytes) {
		replyChunkBytes = bytes;
		return *this;
	}

	QueueConfig& setPreserveReplyUtf8Boundaries(bool preserve) {
		preserveReplyUtf8Boundaries = preserve;
		return *this;
	}
};

struct ExchangeConfig: detail::BaseConfig<ExchangeConfig> {
	std::string type = "";

	ExchangeConfig() = default;

	ExchangeConfig(std::string name, std::string type)
		: BaseConfig(name)
		, type(type)
	{}
};

struct ConnectionConfig {
	int channelPrefetch = 1;
	std::string host = "localhost";
	int port = 5672;
	std::string username = "guest";
	std::string password = "guest";
	int heartbeatInterval = 60;

	ConnectionConfig() = default;

	ConnectionConfig& setChannelPrefetch(int channelPrefetch) {
		this->channelPrefetch = channelPrefetch;
		return *this;
	}
	ConnectionConfig& setHost(std::string host) {
		this->host = host;
		return *this;
	}
	ConnectionConfig& setPort(int port) {
		this->port = port;
		return *this;
	}
	ConnectionConfig& setUsername(std::string username) {
		this->username = username;
		return *this;
	}
	ConnectionConfig& setPassword(std::string password) {
		this->password = password;
		return *this;
	}
	ConnectionConfig& setHeartbeatInterval(int heartbeatInterval) {
		this->heartbeatInterval = heartbeatInterval;
		return *this;
	}
};

/**
 * Use this utility to generate [count] queue configs for an x-random exchange, all with the same handler
 * The names of the queues will be "exchangeName-queue-1", "exchangeName-queue-2", etc.
 */
std::vector<QueueConfig> generateXRandomQueues(std::string const& exchangeName, int count, MQHandler handler);

/** Returns a chunk length without copying the payload. maxChunkBytes must be positive. */
size_t getReplyChunkSize(
	std::string_view remainingPayload,
	size_t maxChunkBytes,
	bool preserveUtf8Boundaries = false
);

} // namespace AMQP
