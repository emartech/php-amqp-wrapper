<?php

namespace Emartech\AmqpWrapper;


use PhpAmqpLib\Message\AMQPMessage;

class MessageBuffer
{
    private $messages = [];

    public function addMessage(AMQPMessage $message): self
    {
        $this->messages[] = new Message($message);
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
