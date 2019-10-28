<?php

use Emartech\AmqpWrapper\Channel;
use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\AmqpWrapper\SimpleConsumer;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class QueueTest extends BaseTestCase implements QueueConsumer
{
    /** @var AbstractConnection */
    private $connection;
    /** @var Factory */
    private $factory;
    /** @var Message[]|array */
    private $consumed = [];
    /** @var Message[]|array */
    private $failed = [];


    public function consume(Message $message): void
    {
        $this->consumed[] = $message;
    }

    public function error(Message $message, Throwable $t): void
    {
        $this->failed[] = $message;
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->factory = Factory::create($this->dummyLogger);
        $this->connection = $this->factory->createConnection($this->getRabbitUrlForTest());
        $this->purgeQueue();
        $this->consumed = [];
        $this->failed = [];
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

        $queue = $this->factory->createQueue($this->getQueueNameForTest());
        $queue->send($message);

        $queue->consume($this);

        $this->assertCount(1, $this->consumed);
        $this->assertCount(0, $this->failed);
        $this->assertEquals($message, $this->consumed[0]->getContents());
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_Error_MessagesRejected()
    {
        $consumer = $this->createMock(QueueConsumer::class);
        $consumer->expects($this->at(0))->method('consume')->willThrowException(new Exception());
        $consumer->expects($this->at(1))->method('consume');

        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        $channel->expects($this->at(0))->method('basic_reject');
        $channel->expects($this->at(1))->method('basic_ack');

        $batchSize = 1;
        $channelWrapper = new Channel($channel, $this->dummyLogger, 'queue_name');
        $messageBuffer = new MessageBuffer($channelWrapper, $batchSize);
        $messageBuffer
            ->addMessage($this->mockRawMessage(['test1']))
            ->addMessage($this->mockRawMessage(['test2']));

        $queue = new Queue('dummy', $channelWrapper, 1, $batchSize, $messageBuffer, $this->dummyLogger);
        $queue->consume(new SimpleConsumer($consumer));
    }

    /**
     * @test
     * @throws ErrorException
     */
    public function consume_MessagesProcessed_MessagesAcknowledged()
    {
        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        $channel->expects($this->exactly(2))->method('basic_ack');

        $batchSize = 1;
        $channelWrapper = new Channel($channel, $this->dummyLogger, 'queue_name');
        $messageBuffer = new MessageBuffer($channelWrapper, $batchSize);
        $messageBuffer
            ->addMessage($this->mockRawMessage(['test1']))
            ->addMessage($this->mockRawMessage(['test2']));

        $queue = new Queue('dummy', $channelWrapper, 1, $batchSize, $messageBuffer, $this->dummyLogger);
        $queue->consume(new SimpleConsumer($this->createMock(QueueConsumer::class)));
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

        $queue->consume($this);

        $this->assertCount(2, $this->consumed);
        $this->assertCount(0, $this->failed);
        $this->assertEquals($message1, $this->consumed[0]->getContents());
        $this->assertEquals($message2, $this->consumed[1]->getContents());
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
        $rawMessage->expects($this->any())->method('getBody')->willReturn($rawMessage->body);

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
}
