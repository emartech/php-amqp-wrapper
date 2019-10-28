<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class Message
{
    private $channel;
    private $message;

    public function __construct(AMQPChannel $channel, AMQPMessage $message)
    {
        $this->message = $message;
        $this->channel = $channel;
    }

    public function getContents(): array
    {
        return json_decode($this->message->getBody(), true);
    }

    public function ack(): void
    {
        $this->channel->basic_ack($this->message->delivery_info['delivery_tag']);
    }

    public function getRawBody(): string
    {
        return $this->message->getBody();
    }

    public function requeue()
    {
        $this->channel->basic_reject($this->message->delivery_info['delivery_tag'], true);
    }
}
