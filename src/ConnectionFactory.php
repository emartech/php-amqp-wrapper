<?php

namespace AmqpWrapper;

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use Psr\Log\LoggerInterface;

class ConnectionFactory
{
    public static function getConnection(string $connectionUrl, LoggerInterface $logger): AbstractConnection
    {
        $url = parse_url($connectionUrl);

        $logger->debug('Connecting to AMQP', array_merge($url, ['pass' => 'secret']));

        switch ($url['scheme']) {
            case 'amqp':
                $connection = new AMQPStreamConnection($url['host'], $url['port'], $url['user'], $url['pass'], substr($url['path'], 1));
                break;
            case 'amqps':
                $sslOptions = [
                    'peer_name' => $url['host'],
                ];

                $options = [
                    'connection_timeout' => 300,
                    'read_write_timeout' => 300,
                ];

                $connection = new AMQPSSLConnection($url['host'], $url['port'], $url['user'], $url['pass'], substr($url['path'], 1), $sslOptions, $options);
                break;
            default:
                throw new AMQPRuntimeException('invalid connection url');
        }

        $logger->debug('Successfully connected to AMQP', ['connectionClass' => get_class($connection)]);

        return $connection;
    }
}
