<?php

namespace Test\Integration;

use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\QueueConsumer;
use Exception;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Test\Helper\SpyConsumer;

class ChannelWrapperTest extends TestCase
{
    private const QUEUE_WAIT_TIMEOUT_SECONDS = 1;

    private string $queueName = 'test_queue';
    private SpyConsumer $spyConsumer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->queueName = 'testing';
        $this->spyConsumer = new SpyConsumer($this);
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
        $this->newChannel(__FUNCTION__)->consume($this->spyConsumer);

        $this->spyConsumer->assertNoMessagesConsumed();
    }

    /**
     * @test
     */
    public function send_MessageSent_MessageCanBeConsumed(): void
    {
        $this->newChannel(__FUNCTION__)->delete();
        $message = ['test'];

        $this->newChannel(__FUNCTION__)->send($message);
        $this->newChannel(__FUNCTION__)->consume($this->spyConsumer);

        $this->spyConsumer->assertConsumedMessagesCount(1);
        $this->spyConsumer->assertConsumedMessage(0, $message);
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
        $this->newChannel(__FUNCTION__, $ttlMilliSeconds)->consume($this->spyConsumer);
        $this->spyConsumer->assertConsumedMessagesCount(1);
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

        $this->newChannel(__FUNCTION__)->consume($this->spyConsumer);

        $this->assertEquals($message1, $this->spyConsumer->consumedMessages[0]->getContents());
        $this->assertEquals($message2, $this->spyConsumer->consumedMessages[1]->getContents());

        $this->newChannel(__FUNCTION__)->consume($this->spyConsumer);

        $this->assertEquals($message3, $this->spyConsumer->consumedMessages[2]->getContents());
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

        $this->newChannel(__FUNCTION__)->consume($this->spyConsumer);

        $this->spyConsumer->assertConsumedMessagesCount(2);
        $this->spyConsumer->assertConsumedMessage(0, $message1);
        $this->spyConsumer->assertConsumedMessage(1, $message2);

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
        $expectedException = new Exception('Some exception happened');
        $mockConsumer->expects($this->any())->method('getPrefetchCount')->willReturn(1);
        $mockConsumer->expects($this->once())->method('consume')->willThrowException($expectedException);

        $this->expectException(get_class($expectedException));
        $this->expectExceptionMessage($expectedException->getMessage());

        $this->newChannel($queueName)->consume($mockConsumer);

        $this->assertNumberOfMessagesLeftInQueue(2, $queueName);
    }

    private function newChannel(string $queueName = null, int $ttlMilliSeconds = null): Queue
    {
        return (new Factory($this->createMock(LoggerInterface::class), getenv('RABBITMQ_URL'), self::QUEUE_WAIT_TIMEOUT_SECONDS))
            ->createQueue($queueName ?: $this->queueName, $ttlMilliSeconds);
    }

    private function assertNumberOfMessagesLeftInQueue(int $expectedCount, string $queueName): void
    {
        $afterSpy = new SpyConsumer($this);
        $this->newChannel($queueName)->consume($afterSpy);
        $afterSpy->assertConsumedMessagesCount($expectedCount);
    }
}
