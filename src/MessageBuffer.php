<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Message\AMQPMessage;

class MessageBuffer
{
    /** @var Channel */
    private $channel;

    /** @var int */
    private $batchSize;

    /** @var Message[] */
    private $messages = [];


    public function __construct(Channel $channel, int $batchSize)
    {
        $this->channel = $channel;
        $this->batchSize = $batchSize;
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

    public function getMessageCount(): int
    {
        return count($this->messages);
    }

    public function isFull(): bool
    {
        return $this->getMessageCount() >= $this->batchSize;
    }
}
