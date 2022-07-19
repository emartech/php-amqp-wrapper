<?php

namespace Test\Integration;

use Emartech\AmqpWrapper\BufferedConsumer;
use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\QueueConsumer;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Test\Helper\SpyConsumer;

class BufferedConsumerTest extends TestCase
{
    private const QUEUE_WAIT_TIMEOUT_SECONDS = 1;
    private const BATCH_SIZE = 2;

    private string $queueName;
    private Queue $queue;
    private SpyConsumer $spyInBufferedConsumer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->queueName = 'testing';
        $this->queue = $this->openQueue();
        $this->queue->purge();
        foreach (range(0, 10) as $i) {
            $this->queue->send(['number' => $i]);
        }

        $this->spyInBufferedConsumer = new SpyConsumer($this);
    }

    /**
     * @test
     */
    public function consume_MessageBufferSizeEqualToPrefetchNumber_AllMessagesConsumedInOneGo()
    {
        $bufferSize = self::BATCH_SIZE;
        $consumer = $this->createBufferedConsumer($bufferSize);

        $this->queue->consume($consumer);
        $this->spyInBufferedConsumer->assertConsumedMessagesCount(11);
    }

    private function createBufferedConsumer(int $bufferSize): BufferedConsumer
    {
        return new BufferedConsumer(
            new MessageBuffer($bufferSize),
            $this->spyInBufferedConsumer,
            $this->createMock(LoggerInterface::class),
            $this->queueName
        );
    }

    protected function openQueue(): Queue
    {
        $factory = new Factory(
            $this->createMock(LoggerInterface::class),
            getenv('RABBITMQ_URL'),
            self::QUEUE_WAIT_TIMEOUT_SECONDS
        );

        return $factory->createQueue($this->queueName);
    }
}
