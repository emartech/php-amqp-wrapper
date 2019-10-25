<?php

namespace Emartech\AmqpWrapper;

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use Psr\Log\LoggerInterface;

class Factory
{
    private const SCHEME_AMQP = 'amqp';
    private const SCHEME_AMQPS = 'amqps';

    private $logger;


    public static function create(LoggerInterface $logger): self
    {
        return new self($logger);
    }

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
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
