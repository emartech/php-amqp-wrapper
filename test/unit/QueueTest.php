<?php

use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class QueueTest extends BaseTestCase
{
    /** @var AbstractConnection */
    private $connection;
    /** @var Factory */
    private $factory;


    protected function setUp(): void
    {
        parent::setUp();
        $this->factory = Factory::create($this->dummyLogger);
        $this->connection = $this->factory->createConnection($this->getRabbitUrlForTest());
        $this->purgeQueue();
    }

    /**
     * @throws Exception
     */
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->connection->close();
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_WaitTimeout_ConsumedMessagesSent()
    {
        $message = ['test1'];

        $consumer = $this->createMock(QueueConsumer::class);
        $consumer->expects($this->at(0))->method('consume')->with([$message]);

        $queue = $this->factory->createQueue($this->getQueueNameForTest());
        $queue->send($message);

        $queue->consume($consumer);
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_Error_MessagesRejected()
    {
        $consumer = $this->createMock(QueueConsumer::class);
        $consumer->expects($this->once())->method('consume')->willThrowException(new Exception());

        $channel = $this->mockChannelToTimeout();
        $channel->expects($this->exactly(2))->method('basic_reject');

        $messageBuffer = $this->mockConsumableMessages([['test1'], ['test2']]);

        $queue = new Queue('dummy', $channel, 1, 1, $messageBuffer, $this->dummyLogger);
        $queue->consume($consumer);
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_MessagesProcessed_MessagesAcknowledged()
    {
        $consumer = $this->createMock(QueueConsumer::class);

        $channel = $this->mockChannelToTimeout();
        $channel->expects($this->exactly(2))->method('basic_ack');

        $messageBuffer = $this->mockConsumableMessages([['test1'], ['test2']]);

        $queue = new Queue('dummy', $channel, 1, 1, $messageBuffer, $this->dummyLogger);
        $queue->consume($consumer);
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_MessagesProcessedNoAckNeeded_MessagesNotAcknowledged()
    {
        $consumer = $this->createMock(QueueConsumer::class);

        $channel = $this->mockChannelToTimeout();
        $channel->expects($this->never())->method('basic_ack');

        $messageBuffer = $this->mockConsumableMessages([['test1'], ['test2']]);

        $queue = new Queue('dummy', $channel, 1, 1, $messageBuffer, $this->dummyLogger);
        $queue->consume($consumer, false);
    }

    /**
     * @test
     * @medium
     * @throws ErrorException
     */
    public function consume_MessagesInQueue_MessagesConsumedOnlyOnce()
    {
        $queue = $this->factory->createQueue($this->getQueueNameForTest());

        $message1 = ['test1'];
        $message2 = ['test2'];
        $queue->send($message1);
        $queue->send($message2);

        $consumer = $this->createMock(QueueConsumer::class);
        $consumer->expects($this->at(0))->method('consume')->with([$message1, $message2]);
        $consumer->expects($this->at(1))->method('consume')->with([]);

        $queue->consume($consumer);
    }

    /**
     * @test
     */
    public function send_MessageSent_MessageIsInQueue()
    {
        $this->factory->createQueue($this->getQueueNameForTest())->send(['test']);
        $this->assertQueueCount(1);
    }

    private function purgeQueue(): void
    {
        $this->factory->openChannel($this->getQueueNameForTest(), $this->getRabbitUrlForTest())->queue_purge($this->getQueueNameForTest());
        $this->factory->openChannel($this->getQueueNameForTest().'.error', $this->getRabbitUrlForTest())->queue_purge($this->getQueueNameForTest().'.error');
    }

    private function assertQueueCount(int $expected): void
    {
        $queueName = $this->getQueueNameForTest();
        $result = $this->factory->openChannel($queueName, $this->getRabbitUrlForTest())
            ->queue_declare($queueName, false, true, false, false);
        $this->assertEquals($expected, $result[1], 'message count mismatch');
        $this->assertEquals(0, $result[2], 'consumer active');
    }

    private function mockRawMessage(array $message): AMQPMessage
    {
        $rawMessage = $this->createMock(AMQPMessage::class);
        $rawMessage->body = json_encode($message);
        $rawMessage->delivery_info['delivery_tag'] = '';

        return $rawMessage;
    }

    private function getQueueNameForTest()
    {
        return getenv('QUEUE_NAME');
    }

    private function getRabbitUrlForTest()
    {
        return getenv('RABBITMQ_URL');
    }

    private function mockChannelToTimeout()
    {
        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        return $channel;
    }

    private function mockConsumableMessages(array $messages): MessageBuffer
    {
        $messageBuffer = new MessageBuffer();

        foreach ($messages as $message) {
            $messageBuffer->addMessage($this->mockRawMessage($message));
        }

        return $messageBuffer;
    }
}
