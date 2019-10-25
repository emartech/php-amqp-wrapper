<?php

use Emartech\AmqpWrapper\ConnectionFactory;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\AmqpWrapper\Queue;
use Emartech\AmqpWrapper\ChannelFactory;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class QueueTest extends BaseTestCase
{
    /** @var ChannelFactory */
    private $channelFactory;
    /** @var AbstractConnection */
    private $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = ConnectionFactory::getConnection(getenv('RABBITMQ_URL'), $this->dummyLogger);
        $this->channelFactory = new ChannelFactory($this->dummyLogger, $this->connection);
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

        $queue = Queue::create(getenv('QUEUE_NAME'), $this->channelFactory, $this->dummyLogger);
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

        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        $channel->expects($this->exactly(2))->method('basic_reject');

        $messageBuffer = new MessageBuffer();
        $messageBuffer
            ->addMessage($this->mockRawMessage(['test1']))
            ->addMessage($this->mockRawMessage(['test2']));

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

        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        $channel->expects($this->exactly(2))->method('basic_ack');

        $messageBuffer = new MessageBuffer();
        $messageBuffer
            ->addMessage($this->mockRawMessage(['test1']))
            ->addMessage($this->mockRawMessage(['test2']));

        $queue = new Queue('dummy', $channel, 1, 1, $messageBuffer, $this->dummyLogger);
        $queue->consume($consumer);
    }

    /**
     * @test
     * @medium
     * @throws ErrorException
     */
    public function consume_MessagesInQueue_MessagesConsumedOnlyOnce()
    {
        $queue = Queue::create(getenv('QUEUE_NAME'), $this->channelFactory, $this->dummyLogger);

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
        Queue::create(getenv('QUEUE_NAME'), $this->channelFactory, $this->dummyLogger)->send(['test']);
        $this->assertQueueCount(1);
    }

    private function purgeQueue(): void
    {
        $this->channelFactory->openQueue(getenv('QUEUE_NAME'))->queue_purge(getenv('QUEUE_NAME'));
        $this->channelFactory->openQueue(getenv('QUEUE_NAME').'.error')->queue_purge(getenv('QUEUE_NAME').'.error');
    }

    private function assertQueueCount(int $expected): void
    {
        $result = $this->channelFactory->openQueue(getenv('QUEUE_NAME'))
            ->queue_declare(getenv('QUEUE_NAME'), false, true, false, false);
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
}
