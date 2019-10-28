<?php

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\MessageBuffer;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Message\AMQPMessage;

class MessageBufferTest extends BaseTestCase
{
    /**
     * @test
     */
    public function allTestCases_Perfect()
    {
        $buffer = new MessageBuffer();

        $message1 = $this->createMock(AMQPMessage::class);
        $buffer->addMessage($message1);
        $message2 = $this->createMock(AMQPMessage::class);
        $buffer->addMessage($message2);

        $this->assertEquals(2, $buffer->getMessageCount());
        $this->assertEquals([new Message($message1), new Message($message2)], $buffer->getMessages());

        $buffer->flush();
        $this->assertEquals(0, $buffer->getMessageCount());
        $this->assertEquals([], $buffer->getMessages());
    }
}
