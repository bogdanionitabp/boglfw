/*
	AMQP manager manages AMQP connection and queue transactions

	Copyright: Bearingpoint
	Created: 09-03-2020
	Author: Bogdan Ionita <bogdan.ionita@bearingpoint.com>
*/

#include "amqp-manager.h"

#include "../utils/log.h"
#include "../utils/ioModif.h"
#include "../utils/strbld.h"
#include "../perf/marker.h"

#include <sstream>
#include <chrono>
#include <thread>
#include <algorithm>

//#define ENABLE_DEBUG_AMQP_LOGS // uncomment to enable debug logs
#define AMQP_LOG_COLOR ioModif::FG_GREEN

#if defined(DEBUG) && defined(ENABLE_DEBUG_AMQP_LOGS)
#define DEBUGAMQPLOG(X) DEBUGLOGLN(AMQP_LOG_COLOR << X)
#else
#define DEBUGAMQPLOG(X)
#endif

#define AMQPLOGLN(X) LOGLN(AMQP_LOG_COLOR << X)

static const size_t MAX_SEND_FRAME_SIZE = 4096; // bytes
static const size_t MAX_REPLY_PAYLOAD_SIZE = 32768; // bytes

void printConnectionStatus(AMQP::Connection *connection) {
	DEBUGAMQPLOG("Connection status: +++++++++++++++++++++++++++++\n"
		<< "vhost: " << connection->vhost() << "\n"
		<< "login: " << connection->login() << "\n"
		<< "channels: " << connection->channels() << "\n"
		<< "initialized: " << connection->initialized() << "\n"
		<< "ready: " << connection->ready() << "\n"
		<< "usable: " << connection->usable() << "\n"
		<< "waiting: " << connection->waiting() << "\n"
	);
}


AMQPManager::AMQPManager(
	std::string const& name,
	AMQP::ConnectionConfig connectionConfig,
	std::vector<AMQP::QueueConfig> mqQueues,
	std::vector<AMQP::ExchangeConfig> mqExchanges,
	std::function<bool()> stopRequested
)
	: name_(name)
	, connectionConfig_(connectionConfig)
	, queues_(std::move(mqQueues))
	, exchanges_(std::move(mqExchanges))
	, stopRequested_(std::move(stopRequested))
{
	LOGPREFIX(strbld() << "AMQP::" << name_);
	// set up the socket connection to the RabbitMQ server
	reconnect();
}

AMQPManager::~AMQPManager() {
	if (amqpChannel_) {
		delete amqpChannel_;
		amqpChannel_ = nullptr;
	}
	if (amqpConnection_)
		delete amqpConnection_;
	if (buffer_) {
		free(buffer_);
		buffer_ = nullptr;
	}
}

void AMQPManager::run(std::function<bool()> idleCallback) {
	bool shouldContinue = true;
	while (shouldContinue) {
		if (!step()) {
			shouldContinue = idleCallback();
		}
	}
}

bool AMQPManager::step() {
	LOGPREFIX(strbld() << "AMQP::" << name_);
	if (!buffer_) {
		// this is the first time we're called, must set up stuff
		amqpMaxFrameSize_ = amqpConnection_->maxFrame();
		bufferSize_ = 2 * amqpMaxFrameSize_;
		buffer_ = (char*)malloc(bufferSize_);
	}

	try {
		const size_t expectedDataSize = amqpConnection_->expected();
		bool socketIdle = false;
		if (bufferWriteOffset_ - bufferReadOffset_ < expectedDataSize) {
			if (expectedDataSize + bufferWriteOffset_ > bufferSize_) {
				// we must increase the buffer to accommodate more data
				bufferSize_ = std::max(2 * bufferSize_, expectedDataSize + bufferWriteOffset_);
				void* newBuffer = realloc(buffer_, bufferSize_);
				if (!newBuffer) {
					// failed to realloc
					throw std::runtime_error("Failed to realloc a larger buffer for AMQP");
				}
				buffer_ = (char*)newBuffer;
			}
			size_t bytesToRead = std::min(net::bytesAvailable(sockConn_), expectedDataSize);
			if (bytesToRead) {
				try {
					net::result res = net::read(sockConn_, buffer_ + bufferWriteOffset_, bufferSize_ - bufferWriteOffset_, bytesToRead);
					if (res != net::result::ok) {
						throw std::runtime_error(strbld() << "Error while reading from socket: " << net::errorString(res));
					}
					timeLastDataReceived_ = std::chrono::system_clock::now();
				} catch (std::exception &e) {
					ERRORLOG("Exception reading " << bytesToRead << " bytes from socket: " << e.what());
					throw;
				}
				bufferWriteOffset_ += bytesToRead;
			} else {
				socketIdle = true;
			}
		}
		const size_t sizeToParse = std::min(expectedDataSize, bufferWriteOffset_ - bufferReadOffset_);
		if (sizeToParse > 0) {
			DEBUGAMQPLOG(EM_ON << "Parsing " << sizeToParse << " bytes of AMQP data." << EM_OFF);
			size_t parsedSize;
			try {
				parsedSize = amqpConnection_->parse(buffer_ + bufferReadOffset_, sizeToParse);
			} catch (std::exception const& err) {
				ERRORLOG("Failed to parse AMQP data (sizeToParse: " << sizeToParse << "): " << err.what());
				throw;
			}
			bufferReadOffset_ += parsedSize;
			if (bufferSize_ - bufferWriteOffset_ < bufferReadOffset_) {
				// there's more unused buffer to the left of the used portion than to the right.
				// we move everything to the beginning of the buffer
				// (there may still be some additional unread data between bufferReadOffset_ and bufferWriteOffset_ that we must not lose)
				auto const oldDataStart = bufferReadOffset_;
				auto const oldDataSize = (bufferWriteOffset_ > bufferReadOffset_) ? bufferWriteOffset_ - bufferReadOffset_ : 0;
				bufferWriteOffset_ = 0;
				bufferReadOffset_ = 0;
				if (oldDataSize) {
					DEBUGAMQPLOG("Recycling " << oldDataSize << " bytes of data while rotating the buffer.");
					memcpy(buffer_, buffer_ + oldDataStart, oldDataSize);
					bufferWriteOffset_ = oldDataSize;
				}
			}
		} else if (socketIdle) {
			// seems we're idle
			verifyTimeouts();
			return false;
		}
	} catch (std::exception &e) {
		ERRORLOG("Exception while reading or parsing AMQP data: " << e.what());
		AMQPLOGLN("Error encountered, reconnecting...");
		reconnect();
	}
	return true;
}

void AMQPManager::reconnect() {
	AMQPLOGLN("Connecting...")
	++channelGeneration_;
	if (socketConnected_) {
		net::closeConnection(sockConn_);
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	initSocketConnection();
	if (amqpChannel_) {
		delete amqpChannel_;
		amqpChannel_ = nullptr;
	}
	if (amqpConnection_) {
		delete amqpConnection_;
		amqpConnection_ = nullptr;
	}
	// set up AMQP protocol
	amqpConnection_ = new AMQP::Connection(this, AMQP::Login(connectionConfig_.username, connectionConfig_.password));
	amqpChannel_ = new AMQP::Channel(amqpConnection_);
	amqpChannel_->onError([this] (const char* err) {
		throw std::runtime_error(std::string("AMQP Channel error: ") + err);
	});
	amqpChannel_->onReady([]() {
		DEBUGAMQPLOG("Channel is READY.");
	});
	amqpChannel_->setQos(connectionConfig_.channelPrefetch, false);
	setupExchanges(exchanges_);
	setupQueues(queues_);
	AMQPLOGLN("Queues and exchanges set up.");

	timeLastHeartbeatSent_ = std::chrono::system_clock::now();
	timeLastDataReceived_ = std::chrono::system_clock::now();
	bufferReadOffset_ = bufferWriteOffset_ = 0;
}

void AMQPManager::initSocketConnection() {
	LOGPREFIX("initSocketConnection");
	socketConnected_ = false;
	do {
		if (stopRequested_ && stopRequested_()) {
			throw AMQPManagerStopRequested();
		}
		AMQPLOGLN("Connecting to RabbitMQ...");
		net::result res = net::connect(connectionConfig_.host, connectionConfig_.port, sockConn_);
		if (res != net::result::ok) {
			ERRORLOG("Unable to connect to RabbitMQ server at " << connectionConfig_.host << ":" << connectionConfig_.port
				<< "\n" << net::errorString(res));
			ERRORLOG("Sleeping for 10 seconds, then we'll retry...");
			for (unsigned step = 0; step < 100; ++step) {
				if (stopRequested_ && stopRequested_()) {
					throw AMQPManagerStopRequested();
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		} else {
			AMQPLOGLN("Connected successfully to RabbitMq.");
			socketConnected_ = true;
		}
	} while (!socketConnected_);
}

void AMQPManager::setupQueues(std::vector<AMQP::QueueConfig> const& queues) {
	LOGPREFIX("setupQueues");
	// declare queues and install queue handlers provided by caller
	for (auto &qConfig : queues) {
		int queueFlags = 0;
		AMQP::Table queueOptions;
		if (qConfig.durable) {
			queueFlags |= AMQP::durable;
		}
		if (qConfig.autodelete) {
			queueFlags |= AMQP::autodelete;
			queueOptions["x-expires"] = 5 * 60 * 1000; // 5 minutes max
		}
		if (qConfig.messageTtl > 0) {
			queueOptions["x-message-ttl"] = qConfig.messageTtl;
		}
		amqpChannel_->declareQueue(qConfig.name, queueFlags, queueOptions)
			.onSuccess([this, &qConfig] (std::string const& qName, uint32_t msgCount, uint32_t csmCount)
		{
			DEBUGAMQPLOG("Queue declared: " << qName);
			if (qConfig.exchangeBinding.exchange != "") {
				AMQPLOGLN("Bind Queue " << qConfig.name << " to exchange " << qConfig.exchangeBinding.exchange << " (" << qConfig.exchangeBinding.routingKey << ")");
				amqpChannel_->bindQueue(qConfig.exchangeBinding.exchange, qConfig.name, qConfig.exchangeBinding.routingKey);
			}
			amqpChannel_->consume(qName).onReceived([this, qName, &qConfig](AMQP::Message const &msg, uint64_t deliveryTag, bool redelivered) {
				// decode the message
				auto payload = std::string(msg.body(), msg.body() + msg.bodySize());
				DEBUGAMQPLOG("Received MQ message on queue '" << qName << "'; correlationId: " << msg.correlationID());

				// extract useful data from the message:
				std::string replyTo = msg.replyTo();
				std::string correlationId = msg.correlationID();
				// The completion callback is bound to the channel generation it was created on,
				// so a reply produced after a reconnect cannot ack or publish on a new channel.
				uint64_t const deliveryChannelGeneration = channelGeneration_;
				// call the handler
				qConfig.handler(payload, [this, deliveryTag, replyTo, correlationId, deliveryChannelGeneration] (std::string result) {
					if (deliveryChannelGeneration != channelGeneration_) {
						AMQPLOGLN("Ignoring completion from a stale AMQP channel generation.");
						return;
					}
					PERF_MARKER("AMQP-send-reply");
					// this is the result callback, which should ALWAYS be invoked from the main thread
					DEBUGAMQPLOG("Sending back reply on queue '" << replyTo << "'");
					size_t bytesSent = 0;
					while (bytesSent < result.size()) {
						size_t messageSize = std::min(result.size() - bytesSent, MAX_REPLY_PAYLOAD_SIZE);
						bool isMultipart = result.size() > MAX_REPLY_PAYLOAD_SIZE;
						bool isLastPart = bytesSent + messageSize == result.size();
						AMQP::Envelope envelope(&result[bytesSent], messageSize);
						envelope.setCorrelationID(correlationId);
						if (isMultipart && !isLastPart) {
							envelope.setTypeName("multipart/incomplete");
						}
						if (amqpChannel_) {
							// send the reply
							amqpChannel_->publish("", replyTo, envelope, AMQP::mandatory);
						}
						bytesSent += messageSize;
					}
					if (amqpChannel_) {
						// acknowledge the initial message so it can be removed from the queue
						amqpChannel_->ack(deliveryTag);
					}
				});
			});
		});
	}
}

void AMQPManager::setupExchanges(std::vector<AMQP::ExchangeConfig> &exchanges) {
	// declare queues and install queue handlers provided by caller
	for (auto &exchange : exchanges) {
		// TODO if needed, implement a translator from string exchange type into enum and use declareExchange as below:
		// amqpChannel_->declareExchange(exchange.name, exchange.type, ...)
	}
}

void AMQPManager::verifyTimeouts() {
	int timeSinceLastDataReceived = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - timeLastDataReceived_).count();
	if (timeSinceLastDataReceived > 5 * connectionConfig_.heartbeatInterval) {
		// something seems wrong, we haven't received anything in a while, so let's reconnect
		AMQPLOGLN("No heartbeat received in " << 5 * connectionConfig_.heartbeatInterval << " seconds, forcing reconnect.");
		reconnect();
	}
	int timeSinceLastHeartbeat = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - timeLastHeartbeatSent_).count();
	if (timeSinceLastHeartbeat >= connectionConfig_.heartbeatInterval) {
		amqpConnection_->heartbeat();
		timeLastHeartbeatSent_ = std::chrono::system_clock::now();
	}
}

// AMQP::ConnectionHandler methods follow :::::::::::::::::::::::

void AMQPManager::onData(AMQP::Connection *connection, const char *data, size_t size) {
	LOGPREFIX(strbld() << "AMQP::" << name_);
	DEBUGAMQPLOG(EM_ON << "AMQP send data (size " << size << ")" << EM_OFF);
	size_t sentSize = 0;
	while (sentSize < size) {
		size_t frameSize = std::min(MAX_SEND_FRAME_SIZE, std::min(amqpMaxFrameSize_, size - sentSize));
		bool success = false;
		try {
			auto res = net::write(sockConn_, data, frameSize);
			if (res != net::result::ok) {
				ERRORLOG("Fail sending data to RabbitMQ (error): " << net::errorString(res));
				ERRORLOG("Data size: " << size << "; frame size: " << frameSize);
			} else {
				success = true;
			}
		} catch (std::exception const& e) {
			ERRORLOG("Fail sending data to RabbitMQ (exception): " << e.what());
		}
		if (success) {
			// reset heartbeat time since any write to the socket counts as a "heartbeat"
			// timeLastHeartbeatSent_ = std::chrono::system_clock::now(); // It seems the above assumption was wrong - the server closes the connection if we don't heartbeat
		} else {
			AMQPLOGLN("Attempting to reconnect...");
			reconnect();
			return;
		}
		sentSize += frameSize;
		data += frameSize;
	}
}

void AMQPManager::onReady(AMQP::Connection *connection) {
	LOGPREFIX(strbld() << "AMQP::" << name_);
	DEBUGAMQPLOG("connection::onready()");
	printConnectionStatus(connection);
}

void AMQPManager::onError(AMQP::Connection *connection, const char *message) {
	LOGPREFIX(strbld() << "AMQP::" << name_);
	ERRORLOG("AMQP disconnected: " << message);
	printConnectionStatus(connection);
	AMQPLOGLN("Attempting to reconnect...");
	reconnect();
}

void AMQPManager::onClosed(AMQP::Connection *connection) {
	LOGPREFIX(strbld() << "AMQP::" << name_);
	onError(connection, "Connection closed by peer.");
}

uint16_t AMQPManager::onNegotiate(AMQP::Connection *connection, uint16_t interval) {
	return connectionConfig_.heartbeatInterval;
}

void AMQPManager::onHeartbeat(AMQP::Connection *connection) {
	LOGPREFIX(strbld() << "AMQP::" << name_);
	timeLastDataReceived_ = std::chrono::system_clock::now();
	DEBUGAMQPLOG("Heartbeat received");
}
