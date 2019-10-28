<?php

namespace Emartech\AmqpWrapper;

use Exception;
use Throwable;

interface QueueConsumer
{
    /**
     * Process the message
     *
     * It is the consumer's responsibility to ack the message after processing it
     * @throws Exception
     */
    public function consume(Message $message): void;

    /**
     * Handle errors
     *
     * It is the consumer's responsibility to reject/requeue the message when this happens
     */
    public function error(Message $message, Throwable $t): void;
}
