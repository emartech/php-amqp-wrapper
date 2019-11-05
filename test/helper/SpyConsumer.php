<?php

namespace Test\helper;


use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\QueueConsumer;
use Exception;

class SpyConsumer implements QueueConsumer
{
    /** @var Message[]|array */
    public $consumedMessages = [];

    /** @var bool */
    public $timeOutCalled = false;

    /**
     * @throws Exception
     */
    public function consume(Message $message): void
    {
        $this->consumedMessages[] = $message;
    }

    public function timeOut(): void
    {
        $this->timeOutCalled = true;
    }

    public function getPrefetchCount(): int
    {
        return 2;
    }
}
