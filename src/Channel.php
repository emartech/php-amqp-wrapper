<?php

namespace Emartech\AmqpWrapper;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class Channel
{
    private $channel;
    private $logger;

    public function __construct(AMQPChannel $channel, LoggerInterface $logger)
    {
        $this->channel = $channel;
        $this->logger = $logger;
    }

    public function ack(AMQPMessage $message): void
    {
        $this->channel->basic_ack($message->delivery_info['delivery_tag']);
    }

    public function requeue(AMQPMessage $message): void
    {
        $this->channel->basic_reject($message->delivery_info['delivery_tag'], true);
    }

    public function publish(AMQPMessage $message)
    {
        $this->channel->basic_publish($message, 'amq.direct');
    }
}
