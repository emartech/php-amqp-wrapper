<?php

namespace Test\Unit;

use Emartech\AmqpWrapper\Factory;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class FactoryTest extends TestCase
{
    /** @var Factory */
    private Factory $factory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->factory = new Factory(
            $this->createMock(LoggerInterface::class),
            getenv('RABBITMQ_URL'),
            1
        );
    }

    /**
     * @test
     */
    public function createConnection_ConnectionIsNotSecure_ProperConnectionObjectReturned()
    {
        $this->assertInstanceOf(
            AMQPStreamConnection::class,
            $this->factory->createConnection($this->getRabbitUrlForTest())
        );
    }

    /**
     * @test
     */
    public function createConnection_BadUrlScheme_ThrowsException()
    {
        $this->expectException(AMQPRuntimeException::class);
        $this->factory->createConnection('invalid://url');
    }

    private function getRabbitUrlForTest(): bool|array|string
    {
        return getenv('RABBITMQ_URL');
    }
}
