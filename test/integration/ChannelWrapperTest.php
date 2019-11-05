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
    /** @var SpyConsumer */
    private $spy;

    protected function setUp(): void
    {
        parent::setUp();
        $this->queueName = 'testing';
        $this->spy = new SpyConsumer($this);
    }

    /**
     * @test
     */
    public function purge_MessagesInQueue_MessagesPurged(): void
    {
        $message1 = ['test1'];
        $message2 = ['test1'];

        $this->newChannel()->send($message1);
        $this->newChannel()->send($message2);
        $this->newChannel()->purge();
        $this->newChannel()->consume($this->spy);

        $this->spy->assertNoMessagesConsumed();
    }

    /**
     * @test
     */
    public function send_MessageSent_MessageCanBeConsumed(): void
    {
        $message = ['test'];

        $this->newChannel()->send($message);
        $this->newChannel()->consume($this->spy);

        $this->spy->assertConsumedMessagesCount(1);
        $this->spy->assertConsumedMessage(0, $message);
    }

    /**
     * @test
     */
    public function send_NumberOfMessagesInQueueExceedsBatchSize_ProcessingOccursInMultipleRounds(): void
    {
        $message1 = ['test1'];
        $message2 = ['test2'];
        $message3 = ['test3'];

        $this->newChannel()->send($message1);
        $this->newChannel()->send($message2);
        $this->newChannel()->send($message3);

        $this->newChannel()->consume($this->spy);

        $this->assertEquals($message1, $this->spy->consumedMessages[0]->getContents());
        $this->assertEquals($message2, $this->spy->consumedMessages[1]->getContents());

        $this->newChannel()->consume($this->spy);

        $this->assertEquals($message3, $this->spy->consumedMessages[2]->getContents());
    }

    /**
     * @test
     */
    public function send_MessagesNotAcknowledgedByConsumer_MessagesLeftHanging(): void
    {
        $message1 = ['test1'];
        $message2 = ['test2'];

        $this->newChannel()->send($message1);
        $this->newChannel()->send($message2);

        $this->newChannel()->consume($this->spy);

        $this->spy->assertConsumedMessagesCount(2);
        $this->spy->assertConsumedMessage(0, $message1);
        $this->spy->assertConsumedMessage(1, $message2);

        $this->assertNumberOfMessagesLeftInQueue(0);
    }

    /**
     * @test
     */
    public function send_ErrorOccursDuringConsumption_ExceptionBubblesUpAndMessageLeftHanging(): void
    {
        $this->newChannel()->send(['test1']);
        $this->newChannel()->send(['test2']);
        $this->newChannel()->send(['test3']);

        $mockConsumer = $this->createMock(QueueConsumer::class);
        $exception = new Exception();
        $mockConsumer->expects($this->any())->method('getPrefetchCount')->willReturn(1);
        $mockConsumer->expects($this->once())->method('consume')
            ->willThrowException($exception);

        $this->assertExceptionThrown($this->identicalTo($exception), function () use ($mockConsumer) {
            $this->newChannel()->consume($mockConsumer);
        });

        $this->assertNumberOfMessagesLeftInQueue(2);
    }

    /**
     * @return Queue
     */
    protected function newChannel(): Queue
    {
        return (new Factory($this->dummyLogger, getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))
            ->createQueue($this->queueName);
    }

    /**
     * @param $expectedCount
     */
    protected function assertNumberOfMessagesLeftInQueue(int $expectedCount): void
    {
        $spy = new SpyConsumer($this);
        $this->newChannel()->consume($spy);
        $spy->assertConsumedMessagesCount($expectedCount);
    }
}
