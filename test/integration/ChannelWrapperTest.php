<?php

namespace Test\integration;


use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\TestHelper\BaseTestCase;
use Exception;
use Test\helper\SpyConsumer;

class ChannelWrapperTest extends BaseTestCase
{
    private const QUEUE_WAIT_TIMEOUT_SECONDS = 1;

    /** @var string */
    private $queueName = 'test_queue';
    /** @var Queue */
    private $queue;
    /** @var SpyConsumer */
    private $consumer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->queueName = 'testing';
        $this->queue = $this->openChannel();
        $this->queue->purge();
        $this->consumer = new SpyConsumer();
    }

    /**
     * @test
     */
    public function purge_MessagesInQueue_MessagesPurged(): void
    {
        $message1 = ['test1'];
        $message2 = ['test1'];

        $this->queue->send($message1);
        $this->queue->send($message2);
        $this->queue->purge();
        $this->queue->consume($this->consumer);

        $this->assertCount(0, $this->consumer->consumedMessages);
    }

    /**
     * @test
     */
    public function send_MessageSent_MessageCanBeConsumed(): void
    {
        $message = ['test'];

        $this->queue->send($message);
        $this->queue->consume($this->consumer);

        $this->assertCount(1, $this->consumer->consumedMessages);
        $this->assertEquals($message, $this->consumer->consumedMessages[0]->getContents());
    }

    /**
     * @test
     */
    public function send_NumberOfMessagesInQueueExceedsBatchSize_ProcessingOccursInMultipleRounds(): void
    {
        $message1 = ['test1'];
        $message2 = ['test2'];
        $message3 = ['test3'];

        $this->queue->send($message1);
        $this->queue->send($message2);
        $this->queue->send($message3);

        $this->queue->consume($this->consumer);

        $this->assertEquals($message1, $this->consumer->consumedMessages[0]->getContents());
        $this->assertEquals($message2, $this->consumer->consumedMessages[1]->getContents());

        $this->queue->consume($this->consumer);

        $this->assertEquals($message3, $this->consumer->consumedMessages[2]->getContents());
    }

    /**
     * @test
     */
    public function send_MessagesNotAcknowledgedByConsumer_MessagesLeftHanging(): void
    {
        $message1 = ['test1'];
        $message2 = ['test2'];

        $this->queue->send($message1);
        $this->queue->send($message2);

        $this->queue->consume($this->consumer);

        $this->assertEquals($message1, $this->consumer->consumedMessages[0]->getContents());
        $this->assertEquals($message2, $this->consumer->consumedMessages[1]->getContents());

        $this->assertNumberOfMessagesLeftInQueue(0);
    }

    /**
     * @test
     */
    public function send_ErrorOccursDuringConsumption_ExceptionBubblesUpAndMessageLeftHanging(): void
    {
        $this->queue->send(['test1']);
        $this->queue->send(['test2']);
        $this->queue->send(['test3']);

        $mockConsumer = $this->createMock(QueueConsumer::class);
        $exception = new Exception();
        $mockConsumer->expects($this->any())->method('getPrefetchCount')->willReturn(1);
        $mockConsumer->expects($this->once())->method('consume')
            ->willThrowException($exception);

        $this->assertExceptionThrown($this->identicalTo($exception), function () use ($mockConsumer) {
            $this->queue->consume($mockConsumer);
        });

        $this->assertNumberOfMessagesLeftInQueue(2);
    }

    /**
     * @return Queue
     */
    protected function openChannel(): Queue
    {
        return (new Factory($this->dummyLogger, getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))
            ->createQueue($this->queueName);
    }

    /**
     * @param $count
     */
    protected function assertNumberOfMessagesLeftInQueue(int $count): void
    {
        $spy = new SpyConsumer();
        $this->openChannel()->consume($spy);
        $this->assertCount($count, $spy->consumedMessages);
    }
}
