<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Message\AMQPMessage;

class MessageBuffer
{
    /** @var Channel */
    private $channel;

    /** @var Message[] */
    private $messages = [];


    public function __construct(Channel $channel)
    {
        $this->channel = $channel;
    }

    public function addMessage(AMQPMessage $message): self
    {
        $this->messages[] = new Message($this->channel, $message);
        return $this;
    }

    /**
     * @return Message[]
     */
    public function getMessages(): array
    {
        return $this->messages;
    }

    public function flush(): void
    {
        $this->messages = [];
    }

    public function getMessageCount()
    {
        return count($this->messages);
    }
}
