<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Message\AMQPMessage;

class Message
{
    private ChannelWrapper $channel;
    private AMQPMessage $message;

    public function __construct(ChannelWrapper $channel, AMQPMessage $message)
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

    public function reject(): void
    {
        $this->channel->reject($this->message);
    }

    public function discard(): void
    {
        $this->channel->discard($this->message);
    }

    public function publish(): void
    {
        $this->channel->publish($this->message);
    }
}
