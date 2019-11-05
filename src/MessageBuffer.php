<?php

namespace Emartech\AmqpWrapper;

class MessageBuffer
{
    /** @var int */
    private $batchSize;

    /** @var Message[] */
    private $messages = [];


    public function __construct(int $batchSize)
    {
        $this->batchSize = $batchSize;
    }

    public function addMessage(Message $message): self
    {
        $this->messages[] = $message;
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

    public function getSize(): int
    {
        return $this->batchSize;
    }
}
