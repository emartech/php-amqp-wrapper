<?php

namespace Emartech\AmqpWrapper;


interface Queue
{
    /**
     * Send a message to the queue
     */
    public function send(array $messageBody): void;

    /**
     * Pass a consumer object to start listening for messages in the queue
     */
    public function consume(QueueConsumer $consumer): void;

    /**
     * Remove all messages from the queue
     */
    public function purge(): void;

    /**
     * Delete the queue
     */
    public function delete(): void;
}
