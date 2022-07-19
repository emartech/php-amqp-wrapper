<?php

namespace Test\integration;

use Emartech\AmqpWrapper\BufferedConsumer;
use Emartech\AmqpWrapper\ChannelWrapper;
use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\QueueConsumer;
use ErrorException;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Test\helper\SpyConsumer;

class BufferedConsumerTest extends TestCase
{
    private const QUEUE_WAIT_TIMEOUT_SECONDS = 1;
    private const BATCH_SIZE = 2;

    /** @var string */
    private $queueName;

    /** @var ChannelWrapper */
    private $queue;

    /** @var SpyConsumer */
    private $spyInBufferedConsumer;

    /** @var SpyConsumer */
    private $spyAfterBufferedConsumption;


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
        $this->spyAfterBufferedConsumption = new SpyConsumer($this);
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_MessageBufferSizeEqualToPrefetchNumber_AllMessagesConsumedInOneGo()
    {
        $bufferSize = self::BATCH_SIZE;
        $consumer = $this->createBufferedConsumer($bufferSize);

        $this->queue->consume($consumer);
        $this->spyInBufferedConsumer->assertConsumedMessagesCount(11);
    }

    private function createBufferedConsumer(int $bufferSize, QueueConsumer $delegate = null): BufferedConsumer
    {
        return new BufferedConsumer(
            new MessageBuffer($bufferSize),
            $delegate ?: $this->spyInBufferedConsumer,
            $this->createMock(LoggerInterface::class),
            $this->queueName
        );
    }

    protected function openQueue(): Queue
    {
        return (new Factory($this->createMock(LoggerInterface::class), getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))->createQueue($this->queueName);
    }
}
