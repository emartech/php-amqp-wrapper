<?php

use Emartech\AmqpWrapper\Factory;
use Emartech\TestHelper\BaseTestCase;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;

class FactoryTest extends BaseTestCase
{
    /** @var Factory */
    private $factory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->factory = Factory::create($this->dummyLogger);
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
    public function createConnection_ConnectionIsSecure_ProperConnectionObjectReturned()
    {
        $this->markTestIncomplete('dev env does not support ssl connections yet');
        $this->assertInstanceOf(AMQPSSLConnection::class, $this->factory->createConnection(str_replace('amqp', 'amqps', $this->getRabbitUrlForTest())));
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
