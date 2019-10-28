<?php

use Emartech\AmqpWrapper\Channel;
use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class MessageBufferTest extends BaseTestCase
{
    /**
     * @test
     */
    public function allTestCases_Perfect()
    {
        /** @var AMQPChannel $channel */
        $channel = $this->mock(AMQPChannel::class);
        $channelWrapper = new Channel($channel, $this->dummyLogger);
        $buffer = new MessageBuffer($channelWrapper);

        $message1 = $this->createMock(AMQPMessage::class);
        $buffer->addMessage($message1);
        $message2 = $this->createMock(AMQPMessage::class);
        $buffer->addMessage($message2);

        $this->assertEquals(2, $buffer->getMessageCount());
        $this->assertEquals([
            new Message($channelWrapper, $message1),
            new Message($channelWrapper, $message2),
        ], $buffer->getMessages());

        $buffer->flush();
        $this->assertEquals(0, $buffer->getMessageCount());
        $this->assertEquals([], $buffer->getMessages());
    }
}
