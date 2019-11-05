<?php

namespace Test\integration;

use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\DumbConsumer;
use Emartech\TestHelper\BaseTestCase;
use Test\helper\SpyConsumer;

class SimpleConsumerTest extends BaseTestCase
{
    private const QUEUE_WAIT_TIMEOUT_SECONDS = 1;

    /**
     * @test
     */
    public function consume_MessagesInQueue_MessagesProcessedAndAcked(): void
    {
        $this->newChannel()->purge();

        $this->newChannel()->send(['number' => 1]);
        $this->newChannel()->send(['number' => 2]);

        $spy = new SpyConsumer($this);
        $this->newChannel()->consume(new DumbConsumer($spy));

        $spy->assertConsumedMessagesCount(2);
        $this->assertQueueIsEmpty();
    }

    private function assertQueueIsEmpty(): void
    {
        $this->assertMessageCountInQueue(0);
    }

    private function newChannel(): Queue
    {
        return (new Factory($this->dummyLogger, getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))
            ->createQueue('testing');
    }

    private function assertMessageCountInQueue(int $expectedCount): void
    {
        $spyConsumer = new SpyConsumer($this);
        $this->newChannel()->consume($spyConsumer);
        $spyConsumer->assertConsumedMessagesCount($expectedCount);
    }
}
