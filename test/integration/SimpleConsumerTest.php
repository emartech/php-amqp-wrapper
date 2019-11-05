<?php

namespace Test\integration;

use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\SimpleConsumer;
use Emartech\TestHelper\BaseTestCase;
use Test\helper\SpyConsumer;

class SimpleConsumerTest extends BaseTestCase
{
    const QUEUE_WAIT_TIMEOUT_SECONDS = 1;
    const BATCH_SIZE = 2;

    /**
     * @test
     */
    public function consume_MessagesInQueue_MessagesProcessedAndAcked(): void
    {
        $queue = $this->openChannel();
        $queue->purge();

        $queue->send(['number' => 1]);
        $queue->send(['number' => 2]);

        $spy = new SpyConsumer();
        $queue->consume(new SimpleConsumer($spy));

        $this->assertCount(2, $spy->consumedMessages);

        $this->assertQueueIsEmpty();
    }

    protected function assertQueueIsEmpty(): void
    {
        $queue = $this->openChannel();
        $spyConsumer = new SpyConsumer();
        $queue->consume($spyConsumer);
        $this->assertCount(0, $spyConsumer->consumedMessages);
    }

    /**
     * @return Queue
     */
    protected function openChannel(): Queue
    {
        return (new Factory($this->dummyLogger, getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))
            ->createQueue('testing');
    }
}
