<?php

namespace Emartech\AmqpWrapper;

use Exception;

interface QueueConsumer
{
    public function consume(array $messages);
    public function error(array $message, Exception $exception);
}
