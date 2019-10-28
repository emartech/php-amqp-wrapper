<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Message\AMQPMessage;

class Message
{
    private $channel;
    private $message;

    public function __construct(Channel $channel, AMQPMessage $message)
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
        $this->channel->ack($this->message);
    }

    public function getRawBody(): string
    {
        return $this->message->getBody();
    }

    public function requeue()
    {
        $this->channel->requeue($this->message);
    }
}
