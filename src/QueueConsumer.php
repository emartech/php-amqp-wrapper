<?php

namespace AmqpWrapper;

use ContactList\Api\Amqp\AMQPRejectItemException;

interface QueueConsumer
{
    public function consume(array $messages);
    public function error(array $message, \Exception $exception);
}
