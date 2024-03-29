<?php

namespace Test\Unit;

use Emartech\AmqpWrapper\BufferedConsumer;
use Emartech\AmqpWrapper\ChannelWrapper;
use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\AmqpWrapper\QueueConsumer;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Test\Helper\SpyConsumer;

class QueueTest extends TestCase
{
    private AbstractConnection $connection;
    private Factory $factory;
    private SpyConsumer $spyConsumer;
    private LoggerInterface $logger;

    protected function setUp(): void
    {
        parent::setUp();
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->factory = new Factory($this->logger, getenv('RABBITMQ_URL'), 1);
        $this->connection = $this->factory->createConnection($this->getRabbitUrlForTest());
        $this->spyConsumer = new SpyConsumer($this);
        $this->purgeQueue();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->connection->close();
    }

    /**
     * @test
     */
    public function send_MessageSent_MessageIsInQueue()
    {
        $this->factory->createQueue($this->getQueueNameForTest())->send(['test']);
        $this->assertQueueCount();
    }

    /**
     * @test
     */
    public function consume_WaitTimeout_ConsumedMessagesSent()
    {
        $message = ['test1'];

        $queue = $this->factory->createQueue($this->getQueueNameForTest());

        $queue->send($message);
        $queue->consume($this->spyConsumer);

        $this->assertCount(1, $this->spyConsumer->consumedMessages);
        $this->assertEquals($message, $this->spyConsumer->consumedMessages[0]->getContents());
    }

    /**
     * @test
     */
    public function consume_MessagesProcessed_MessagesAcknowledged()
    {
        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        $channel->expects($this->exactly(2))->method('basic_ack');

        $batchSize = 1;
        $channelWrapper = new ChannelWrapper($channel, $this->logger, $this->getQueueNameForTest(), 1);
        $messageBuffer = new MessageBuffer($batchSize);
        $messageBuffer
            ->addMessage(new Message($channelWrapper, $this->mockRawMessage(['test1'])))
            ->addMessage(new Message($channelWrapper, $this->mockRawMessage(['test2'])));

        $channelWrapper->consume(new BufferedConsumer($messageBuffer, $this->createMock(QueueConsumer::class), $this->logger, $this->getQueueNameForTest()));
    }

    /**
     * @test
     * @medium
     */
    public function consume_MessagesInQueue_MessagesConsumedOnlyOnce()
    {
        $queue = $this->factory->createQueue($this->getQueueNameForTest());

        $message1 = ['test1'];
        $message2 = ['test2'];
        $queue->send($message1);
        $queue->send($message2);

        $queue->consume($this->spyConsumer);

        $this->assertCount(2, $this->spyConsumer->consumedMessages);
        $this->assertEquals($message1, $this->spyConsumer->consumedMessages[0]->getContents());
        $this->assertEquals($message2, $this->spyConsumer->consumedMessages[1]->getContents());
    }

    private function purgeQueue(): void
    {
        $this->factory->openChannel(
            $this->getQueueNameForTest(),
            $this->getRabbitUrlForTest())->queue_purge($this->getQueueNameForTest()
        );
        $this->factory
            ->openChannel($this->getQueueNameForTest().'.error', $this->getRabbitUrlForTest())
            ->queue_purge($this->getQueueNameForTest().'.error');
    }

    private function assertQueueCount(): void
    {
        $queueName = $this->getQueueNameForTest();
        $result = $this->factory->openChannel($queueName, $this->getRabbitUrlForTest())
            ->queue_declare($queueName, false, true, false, false);
        $this->assertEquals(1, $result[1], 'message count mismatch');
    }

    private function mockRawMessage(array $message): AMQPMessage
    {
        $rawMessage = $this->createMock(AMQPMessage::class);
        $rawMessage->body = json_encode($message);
        $rawMessage->setDeliveryInfo($rawMessage->getDeliveryTag(), false, '', '');
        $rawMessage->expects($this->any())->method('getBody')->willReturn($rawMessage->body);

        return $rawMessage;
    }

    private function getQueueNameForTest(): string
    {
        return 'testing';
    }

    private function getRabbitUrlForTest(): string
    {
        return getenv('RABBITMQ_URL');
    }
}
