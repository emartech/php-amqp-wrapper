<?php

namespace Emartech\AmqpWrapper;


use Exception;
use Throwable;

/**
 * This consumer encapsulates the former behavior of the wrapper library,
 * ie. it automatically acknowledges processed messages and automatically rejects failed ones
 */
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
        $this->delegate->consume($message);
        $message->ack();
    }

    public function error(Message $message, Throwable $t): void
    {
        $message->requeue();
        $this->delegate->error($message, $t);
    }
}
