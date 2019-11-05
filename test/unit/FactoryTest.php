<?php

use Emartech\AmqpWrapper\Factory;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;

class FactoryTest extends BaseTestCase
{
    /** @var Factory */
    private $factory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->factory = (new Factory($this->dummyLogger, getenv('RABBITMQ_URL'), 1));
    }

    /**
     * @test
     */
    public function createConnection_ConnectionIsNotSecure_ProperConnectionObjectReturned()
    {
        $this->assertInstanceOf(AMQPStreamConnection::class, $this->factory->createConnection($this->getRabbitUrlForTest()));
    }

    /**
     * @test
     */
    public function createConnection_BadUrlScheme_ThrowsException()
    {
        $this->assertExceptionThrown(AMQPRuntimeException::class, function () {
            $this->factory->createConnection('invalid://url');
        });
    }

    /**
     * @return array|false|string
     */
    private function getRabbitUrlForTest()
    {
        return getenv('RABBITMQ_URL');
    }
}
