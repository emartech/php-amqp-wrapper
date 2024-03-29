<?php

namespace Emartech\AmqpWrapper;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Wire\AMQPTable;
use Psr\Log\LoggerInterface;

class Factory
{
    private const SCHEME_AMQP = 'amqp';
    private const SCHEME_AMQPS = 'amqps';

    private LoggerInterface $logger;
    private string $connectionUrl;
    private int $waitTimeout;
    /** @var Queue[] */
    private array $connections = [];


    public function __construct(LoggerInterface $logger, string $connectionUrl, int $waitTimeout)
    {
        $this->logger = $logger;
        $this->connectionUrl = $connectionUrl;
        $this->waitTimeout = $waitTimeout;
    }

    public function closeQueue(string $queueName)
    {
        $this->connections[$queueName]->close();
        unset($this->connections[$queueName]);
    }

    public function createQueue(string $queueName, int $ttlMilliSeconds = null): Queue
    {
        if (!isset($this->connections[$queueName])) {
            $this->connections[$queueName] = new ChannelWrapper(
                $this->openChannel($queueName, $this->connectionUrl, $ttlMilliSeconds),
                $this->logger,
                $queueName,
                $this->waitTimeout
            );
        }

        return $this->connections[$queueName];
    }

    public function openChannel(string $queueName, string $connectionUrl, int $ttlMilliSeconds = null): AMQPChannel
    {
        $this->logger->debug('Connecting to AMQP', ['queue' => $queueName]);

        $channel = $this->createConnection($connectionUrl)->channel();
        $queueArguments = [];
        if (null !== $ttlMilliSeconds) {
            $queueArguments['x-message-ttl'] = $ttlMilliSeconds;
        }
        $channel->queue_declare($queueName, false, true, false, false, false, new AMQPTable($queueArguments));

        $this->logger->debug('Successfully connected to AMQP', ['queue' => $queueName, 'channelId' => $channel->getChannelId()]);

        return $channel;
    }

    public function createConnection(string $connectionUrl): AbstractConnection
    {
        $url = parse_url($connectionUrl);
        $this->logConnectionAttempt($url);

        switch ($url['scheme']) {
            case self::SCHEME_AMQP:
                return $this->createUnencryptedConnection($url);
            case self::SCHEME_AMQPS:
                return $this->createSSLConnection($url);
            default:
                throw new AMQPRuntimeException('invalid connection url');
        }
    }

    private function createUnencryptedConnection(array $url): AMQPStreamConnection
    {
        $connection = new AMQPStreamConnection(
            $url['host'],
            $url['port'],
            $url['user'],
            $url['pass'],
            substr($url['path'], 1)
        );
        $this->logConnectionSuccess($connection);
        return $connection;
    }

    private function createSSLConnection($url): AMQPSSLConnection
    {
        $sslOptions = [
            'peer_name' => $url['host'],
        ];

        $options = [
            'connection_timeout' => 300,
            'read_write_timeout' => 300,
        ];

        $connection = new AMQPSSLConnection($url['host'], $url['port'], $url['user'], $url['pass'], substr($url['path'], 1), $sslOptions, $options);
        $this->logConnectionSuccess($connection);
        return $connection;
    }

    private function logConnectionAttempt(array $url): void
    {
        $this->logger->debug('Connecting to AMQP', array_merge($url, ['pass' => 'secret']));
    }

    private function logConnectionSuccess(AbstractConnection $connection): void
    {
        $this->logger->debug('Successfully connected to AMQP', ['connection_class' => get_class($connection)]);
    }
}
