<?php

namespace Emartech\AmqpWrapper;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use Psr\Log\LoggerInterface;

class Factory
{
    private const EXCHANGE_DIRECT = 'amq.direct';
    private const SCHEME_AMQP = 'amqp';
    private const SCHEME_AMQPS = 'amqps';

    private $logger;
    private $connectionUrl;
    private $waitTimeout;


    public function __construct(LoggerInterface $logger, string $connectionUrl, int $waitTimeout)
    {
        $this->logger = $logger;
        $this->connectionUrl = $connectionUrl;
        $this->waitTimeout = $waitTimeout;
    }

    public function createQueue(string $queueName): Queue
    {
        return new ChannelWrapper(
            $this->openChannel($queueName, $this->connectionUrl), $this->logger, $queueName, $this->waitTimeout
        );
    }

    public function openChannel(string $queueName, string $connectionUrl): AMQPChannel
    {
        $this->logger->debug('Connecting to AMQP', ['queue' => $queueName]);

        $channel = $this->createConnection($connectionUrl)->channel();
        $channel->queue_declare($queueName, false, true, false, false);
        $channel->exchange_declare(self::EXCHANGE_DIRECT, 'direct', true, true, false);
        $channel->queue_bind($queueName, self::EXCHANGE_DIRECT);

        $this->logger->debug('Successfully connected to AMQP', ['queue' => $queueName]);

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

    /**
     * @return AMQPStreamConnection
     */
    private function createUnencryptedConnection(array $url): AMQPStreamConnection
    {
        $connection = new AMQPStreamConnection($url['host'], $url['port'], $url['user'], $url['pass'], substr($url['path'], 1));
        $this->logConnectionSuccess($connection);
        return $connection;
    }

    /**
     * @return AMQPSSLConnection
     */
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
