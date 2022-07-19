<?php

namespace Emartech\AmqpWrapper;

use Exception;

interface QueueConsumer
{
    public function getPrefetchCount(): ?int;

    /**
     * Process the message
     *
     * It is the consumer's responsibility to ack the message after processing it.
     * It is also the consumer's responsibility to catch any processing related errors or exceptions
     * and handle them properly (requeue them for example)
     *
     * @throws Exception
     */
    public function consume(Message $message): void;

    /**
     * Gets called after consumer timeout
     */
    public function timeOut(): void;
}
