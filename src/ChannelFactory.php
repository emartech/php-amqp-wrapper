<?php

namespace Emartech\AmqpWrapper;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use Psr\Log\LoggerInterface;

class ChannelFactory
{
    const EXCHANGE = 'amq.direct';

    private $logger;
    private $connection;

    public static function create(LoggerInterface $logger): self
    {
        return new self($logger, ConnectionFactory::getConnection(getenv('RABBITMQ_URL'), $logger));
    }

    public function __construct(LoggerInterface $logger, AbstractConnection $connection)
    {
        $this->logger = $logger;
        $this->connection = $connection;
    }

    public function openQueue(string $queueName): AMQPChannel
    {
        $this->logger->debug('Connecting to AMQP', ['queue' => $queueName]);

        $channel = $this->connection->channel();
        $channel->queue_declare($queueName, false, true, false, false);
        $channel->exchange_declare(self::EXCHANGE, 'direct', true, true, false);
        $channel->queue_bind($queueName, self::EXCHANGE);

        $this->logger->debug('Successfully connected to AMQP', ['queue' => $queueName]);

        return $channel;
    }
}
