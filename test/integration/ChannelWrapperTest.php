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
        $this->newChannel(__FUNCTION__)->delete();
        $message1 = ['test1'];
        $message2 = ['test2'];

        $this->newChannel(__FUNCTION__)->send($message1);
        $this->newChannel(__FUNCTION__)->send($message2);
        $this->newChannel(__FUNCTION__)->purge();
        $this->newChannel(__FUNCTION__)->consume($this->spy);

        $this->spy->assertNoMessagesConsumed();
    }

    /**
     * @test
     */
    public function send_MessageSent_MessageCanBeConsumed(): void
    {
        $this->newChannel(__FUNCTION__)->delete();
        $message = ['test'];

        $this->newChannel(__FUNCTION__)->send($message);
        $this->newChannel(__FUNCTION__)->consume($this->spy);

        $this->spy->assertConsumedMessagesCount(1);
        $this->spy->assertConsumedMessage(0, $message);
    }

    /**
     * @test
     */
    public function send_TtlSetForQueue_AfterTtlPassesMessageIsAutomaticallyDiscarded(): void
    {
        $ttlMilliSeconds = 500;
        $this->newChannel(__FUNCTION__, $ttlMilliSeconds)->delete();
        $message = ['test'];

        $this->newChannel(__FUNCTION__, $ttlMilliSeconds)->send($message);
        $this->newChannel(__FUNCTION__, $ttlMilliSeconds)->consume($this->spy);
        $this->spy->assertConsumedMessagesCount(1);
        usleep(600);

        $afterSpy = new SpyConsumer($this);
        $this->newChannel(__FUNCTION__, $ttlMilliSeconds)->consume($afterSpy);
        $afterSpy->assertNoMessagesConsumed();
    }

    /**
     * @test
     */
    public function send_NumberOfMessagesInQueueExceedsBatchSize_ProcessingOccursInMultipleRounds(): void
    {
        $this->newChannel(__FUNCTION__)->delete();
        $message1 = ['test1'];
        $message2 = ['test2'];
        $message3 = ['test3'];

        $this->newChannel(__FUNCTION__)->send($message1);
        $this->newChannel(__FUNCTION__)->send($message2);
        $this->newChannel(__FUNCTION__)->send($message3);

        $this->newChannel(__FUNCTION__)->consume($this->spy);

        $this->assertEquals($message1, $this->spy->consumedMessages[0]->getContents());
        $this->assertEquals($message2, $this->spy->consumedMessages[1]->getContents());

        $this->newChannel(__FUNCTION__)->consume($this->spy);

        $this->assertEquals($message3, $this->spy->consumedMessages[2]->getContents());
    }

    /**
     * @test
     */
    public function send_MessagesNotAcknowledgedByConsumer_MessagesLeftHanging(): void
    {
        $this->newChannel(__FUNCTION__)->delete();
        $message1 = ['test1'];
        $message2 = ['test2'];

        $this->newChannel(__FUNCTION__)->send($message1);
        $this->newChannel(__FUNCTION__)->send($message2);

        $this->newChannel(__FUNCTION__)->consume($this->spy);

        $this->spy->assertConsumedMessagesCount(2);
        $this->spy->assertConsumedMessage(0, $message1);
        $this->spy->assertConsumedMessage(1, $message2);

        $this->assertNumberOfMessagesLeftInQueue(0, __FUNCTION__);
    }

    /**
     * @test
     */
    public function send_ErrorOccursDuringConsumption_ExceptionBubblesUpAndMessageLeftHanging(): void
    {
        $queueName = __FUNCTION__;
        $this->newChannel($queueName)->delete();
        $this->newChannel($queueName)->send(['test1']);
        $this->newChannel($queueName)->send(['test2']);
        $this->newChannel($queueName)->send(['test3']);

        $mockConsumer = $this->createMock(QueueConsumer::class);
        $exception = new Exception();
        $mockConsumer->expects($this->any())->method('getPrefetchCount')->willReturn(1);
        $mockConsumer->expects($this->once())->method('consume')->willThrowException($exception);

        $this->assertExceptionThrown($this->identicalTo($exception), function () use ($mockConsumer, $queueName) {
            $this->newChannel($queueName)->consume($mockConsumer);
        });

        $this->assertNumberOfMessagesLeftInQueue(2, $queueName);
    }

    private function newChannel(string $queueName = null, int $ttlMilliSeconds = null): Queue
    {
        return (new Factory($this->dummyLogger, getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))
            ->createQueue($queueName ?: $this->queueName, $ttlMilliSeconds);
    }

    private function assertNumberOfMessagesLeftInQueue(int $expectedCount, string $queueName): void
    {
        $afterSpy = new SpyConsumer($this);
        $this->newChannel($queueName)->consume($afterSpy);
        $afterSpy->assertConsumedMessagesCount($expectedCount);
    }
}
