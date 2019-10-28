<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class Message
{
    private $message;

    public function __construct(AMQPMessage $message)
    {
        $this->message = $message;
    }

    public function getContents(): array
    {
        return json_decode($this->message->getBody(), true);
    }

    public function ack(AMQPChannel $channel): void
    {
        $channel->basic_ack($this->message->delivery_info['delivery_tag']);
    }

    public function getRawBody(): string
    {
        return $this->message->getBody();
    }

    public function requeue(AMQPChannel $channel)
    {
        $channel->basic_reject($this->message->delivery_info['delivery_tag'], true);
    }
}
