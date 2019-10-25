<?php

use Emartech\AmqpWrapper\ConnectionFactory;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use Test\BaseTestCase;

class ConnectionFactoryTest extends BaseTestCase
{
    /**
     * @test
     */
    public function getConnection_ConnectionIsNotSecure_ProperConnectionObjectReturned()
    {
        $this->assertInstanceOf(AMQPStreamConnection::class, ConnectionFactory::getConnection(getenv('RABBITMQ_URL'), $this->dummyLogger));
    }

    /**
     * @test
     */
    public function getConnection_ConnectionIsSecure_ProperConnectionObjectReturned()
    {
        $this->markTestIncomplete('dev env does not support ssl connections yet');
        $this->assertInstanceOf(AMQPSSLConnection::class, ConnectionFactory::getConnection(str_replace('amqp', 'amqps', getenv('RABBITMQ_URL')), $this->dummyLogger));
    }

    /**
     * @test
     */
    public function getConnection_BadUrlScheme_ThrowsException()
    {
        $this->expectException(AMQPRuntimeException::class);
        ConnectionFactory::getConnection('invalid://url', $this->dummyLogger);
    }
}
