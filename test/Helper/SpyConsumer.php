<?php

namespace Test\Helper;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\QueueConsumer;
use Exception;
use PHPUnit\Framework\TestCase;

class SpyConsumer implements QueueConsumer
{
    /** @var Message[] */
    public array $consumedMessages = [];
    public bool $timeOutCalled = false;
    private TestCase $testCase;
    private int $prefetchCount;

    public function __construct(TestCase $testCase, int $prefetchCount = 2)
    {
        $this->testCase = $testCase;
        $this->prefetchCount = $prefetchCount;
    }

    public function consume(Message $message): void
    {
        $this->consumedMessages[] = $message;
    }

    public function timeOut(): void
    {
        $this->timeOutCalled = true;
    }

    public function getPrefetchCount(): int
    {
        return $this->prefetchCount;
    }

    public function assertNoMessagesConsumed(): void
    {
        $this->assertConsumedMessagesCount(0);
    }

    public function assertConsumedMessagesCount(int $expectedCount): void
    {
        $this->testCase->assertCount($expectedCount, $this->consumedMessages);
    }

    public function assertConsumedMessage(int $index, array $expectedRawMessageContents): void
    {
        $this->testCase->assertEquals($expectedRawMessageContents, $this->consumedMessages[$index]->getContents());
    }
}
