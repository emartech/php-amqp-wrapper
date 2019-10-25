<?php

namespace AmqpWrapper;

interface QueueConsumer
{
    public function consume(array $messages);
    public function error(array $message, \Exception $exception);
}
