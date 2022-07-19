<?php

namespace Test\Unit;

use Emartech\AmqpWrapper\ChannelWrapper;
use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\MessageBuffer;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class MessageBufferTest extends TestCase
{
    /**
     * @test
     */
    public function allTestCases_Perfect()
    {
        $batchSize = 123;

        /** @var AMQPChannel $channel */
        $channel = $this->createMock(AMQPChannel::class);
        $channelWrapper = new ChannelWrapper(
            $channel,
            $this->createMock(LoggerInterface::class),
            'queue_name',
            1
        );
        $buffer = new MessageBuffer($batchSize);

        $message1 = $this->createMock(AMQPMessage::class);
        $buffer->addMessage(new Message($channelWrapper, $message1));
        $message2 = $this->createMock(AMQPMessage::class);
        $buffer->addMessage(new Message($channelWrapper, $message2));

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
