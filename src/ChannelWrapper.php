<?php

namespace Emartech\AmqpWrapper;

use ErrorException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class ChannelWrapper implements Queue
{
    private $channel;
    private $logger;
    private $queueName;
    private $timeOut;


    public function __construct(AMQPChannel $channel, LoggerInterface $logger, string $queueName, int $timeOut)
    {
        $this->channel = $channel;
        $this->logger = $logger;
        $this->queueName = $queueName;
        $this->timeOut = $timeOut;
    }

    public function send(array $contents): void
    {
        $this->wrapAmqpMessage($this->createAmqpMessage($contents))->publish();
    }

    public function purge(): void
    {
        $this->channel->queue_purge($this->queueName);
    }

    /**
     * @throws ErrorException
     */
    public function consume(QueueConsumer $consumer): void
    {
        $consumerTag = 'consumer' . getmypid();
        $prefetchCount = $consumer->getPrefetchCount();
        if (null !== $prefetchCount) {
            $this->channel->basic_qos(0, $prefetchCount, false);
        }
        $this->channel->basic_consume($this->queueName, $consumerTag, false, false, false, false, function (AMQPMessage $amqpMessage) use ($consumer) {
            $this->logDebug('consume_prepare', $amqpMessage->getBody(), 'Consuming message');
            $consumer->consume($this->wrapAmqpMessage($amqpMessage));
        });

        try {
            do {
                $this->channel->wait(null, false, $this->timeOut);
            } while ($this->channel->is_consuming());
        } catch (AMQPTimeoutException $e) {
            $consumer->timeOut();
        }

        $this->channel->basic_cancel($consumerTag);
    }

    public function ack(AMQPMessage $message): void
    {
        $this->channel->basic_ack($message->delivery_info['delivery_tag']);
        $this->logDebug('message_ack', $message->getBody(), 'ACK-ing message');
    }

    public function reject(AMQPMessage $message): void
    {
        $this->channel->basic_reject($message->delivery_info['delivery_tag'], true);
        $this->logDebug('message_rejected', $message->getBody(), 'Rejecting message');
    }

    public function discard(AMQPMessage $message): void
    {
        $this->channel->basic_reject($message->delivery_info['delivery_tag'], false);
        $this->logDebug('message_discarded', $message->getBody(), 'Discarding message');
    }

    public function publish(AMQPMessage $message)
    {
        $this->channel->basic_publish($message, 'amq.direct');
        $this->logDebug('message_sent', $message->getBody(), 'AMQP message sent');
    }

    private function wrapAmqpMessage(AMQPMessage $message): Message
    {
        return new Message($this, $message);
    }

    private function createAmqpMessage(array $contents): AMQPMessage
    {
        return new AMQPMessage(
            json_encode($contents),
            [
                'content_type' => 'text/plain',
                'delivery_mode' => 2,
            ]
        );
    }

    private function logDebug(string $event, string $rawMessage, string $logMessage): void
    {
        $this->logger->debug($logMessage, [
            'queue' => $this->queueName,
            'event' => $event,
            'raw_message' => $rawMessage,
        ]);
    }

}
