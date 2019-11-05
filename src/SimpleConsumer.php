<?php

namespace Emartech\AmqpWrapper;

use Exception;
use Throwable;

class SimpleConsumer implements QueueConsumer
{
    private $delegate;


    public function __construct(QueueConsumer $delegate)
    {
        $this->delegate = $delegate;
    }

    /**
     * @throws Exception
     */
    public function consume(Message $message): void
    {
        try {
            $this->delegate->consume($message);
            $message->ack();
        } catch (Throwable $t) {
            $message->requeue();
        }
    }

    public function timeOut(): void
    {
    }

    public function getPrefetchCount(): int
    {
        return 1;
    }
}
