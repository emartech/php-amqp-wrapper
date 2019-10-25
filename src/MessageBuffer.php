<?php

namespace Emartech\AmqpWrapper;


class MessageBuffer
{
    private $messages = [];

    public function addMessage($message): self
    {
        $this->messages[] = $message;
        return $this;
    }

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
