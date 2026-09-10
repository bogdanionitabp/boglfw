/*
	AMQP manager manages AMQP connection and queue transactions

	Copyright: Bearingpoint
	Created: 09-03-2020
	Author: Bogdan Ionita <bogdan.ionita@bearingpoint.com>
*/

#pragma once

#include "amqp.h"
#include "../net/connection.h"

#include <amqpcpp.h>

#include <functional>
#include <chrono>
#include <cstdint>
#include <stdexcept>

class AMQPManagerStopRequested : public std::runtime_error {
public:
	AMQPManagerStopRequested()
		: std::runtime_error("AMQP connection stopped") {}
};

/**
 * This class IS NOT THREAD SAFE !!!
 * Create it and call its methods ON THE SAME THREAD ALWAYS !!!
 * This is due to the nature of the underlying AMQP library which does not support multi-threading.
*/
class AMQPManager : private AMQP::ConnectionHandler {
public:
	AMQPManager(
		std::string const& name,
		AMQP::ConnectionConfig connectionConfig,
		std::vector<AMQP::QueueConfig> mqQueues,
		std::vector<AMQP::ExchangeConfig> mqExchanges = {},
		std::function<bool()> stopRequested = {}
	);
	~AMQPManager();

	AMQPManager(AMQPManager const& other) = delete;
	AMQPManager(AMQPManager&& other) = delete;

	/**
	 * Process any pending data and performs network communication with the AMQP server.
	 * All messages are being sent and received during this function.
	 * Message handler callbacks are called from this function.
	 *
	 * !!! This function must be called in a loop at short intervals throughout the application's lifetime !!!
	 *
	 * @returns true if any data was processed, false otherwise (if the queue is idle at the moment)
	*/
	bool step();

	/**
	 * Convenience function; runs step() in a loop and calls idleCallback each time the queue is idle
	 * @param idleCallback a callback that is called when the queue is idle. Must return true if the processing should continue
	 * or false if the loop should be terminated.
	 */
	void run(std::function<bool()> idleCallback);

private:
	void setupQueues(std::vector<AMQP::QueueConfig> const& queues);
	void setupExchanges(std::vector<AMQP::ExchangeConfig> &exchanges);
	void initSocketConnection();
	void reconnect();
	void verifyTimeouts();

	std::string name_;
	AMQP::ConnectionConfig connectionConfig_;
	net::connection sockConn_ = nullptr;
	bool socketConnected_ = false;
	AMQP::Connection *amqpConnection_ = nullptr;
	AMQP::Channel* amqpChannel_ = nullptr;
	std::vector<AMQP::QueueConfig> queues_;
	std::vector<AMQP::ExchangeConfig> exchanges_;
	std::function<bool()> stopRequested_;
	uint64_t channelGeneration_ = 0;
	std::chrono::system_clock::time_point timeLastHeartbeatSent_;
	std::chrono::system_clock::time_point timeLastDataReceived_;

	size_t bufferReadOffset_ = 0;
	size_t bufferWriteOffset_ = 0;
	size_t amqpMaxFrameSize_ = 256; // just a safe number to make sure we don't exceed the frame size while negociating. It will be changed after that
	size_t bufferSize_ = 0;
	char* buffer_ = nullptr;

	/**************** AMQP::ConnectionHandler methods *******************/

	/**
	 *  Method that is called by the AMQP library every time it has data
	 *  available that should be sent to RabbitMQ.
	 *  @param  connection  pointer to the main connection object
	 *  @param  data        memory buffer with the data that should be sent to RabbitMQ
	 *  @param  size        size of the buffer
	 */
	void onData(AMQP::Connection *connection, const char *data, size_t size) override;

	/**
	 *  Method that is called by the AMQP library when the login attempt
	 *  succeeded. After this method has been called, the connection is ready
	 *  to use.
	 *  @param  connection      The connection that can now be used
	 */
	void onReady(AMQP::Connection *connection) override;

	 /**
	 *  Method that is called by the AMQP library when a fatal error occurs
	 *  on the connection, for example because data received from RabbitMQ
	 *  could not be recognized.
	 *  @param  connection      The connection on which the error occured
	 *  @param  message         A human readable error message
	 */
	void onError(AMQP::Connection *connection, const char *message) override;

	/**
	 *  Method that is called when the connection was closed. This is the
	 *  counter part of a call to Connection::close() and it confirms that the
	 *  AMQP connection was correctly closed.
	 *
	 *  @param  connection      The connection that was closed and that is now unusable
	 */
	void onClosed(AMQP::Connection *connection) override;

	/**
	 *  Method that is called when the server tries to negotiate a heartbeat
	 *  interval, and that is overridden to get rid of the default implementation
	 *  (which vetoes the suggested heartbeat interval), and accept the interval
	 *  instead.
	 *  @param  connection      The connection on which the error occurred
	 *  @param  interval        The suggested interval in seconds
	 */
	uint16_t onNegotiate(AMQP::Connection *connection, uint16_t interval) override;

	void onHeartbeat(AMQP::Connection *connection) override;
};
