<?php

namespace Emartech\AmqpWrapper;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class ChannelWrapper implements Queue
{
    private AMQPChannel $channel;
    private LoggerInterface $logger;
    private string $queueName;
    private int $timeOut;


    public function __construct(AMQPChannel $channel, LoggerInterface $logger, string $queueName, int $timeOut)
    {
        $this->channel = $channel;
        $this->logger = $logger;
        $this->queueName = $queueName;
        $this->timeOut = $timeOut;
    }

    public function close(): void
    {
        $this->channel->getConnection()->close();
    }

    public function send(array $messageBody): void
    {
        $this->wrapAmqpMessage($this->createAmqpMessage($messageBody))->publish();
    }

    public function purge(): void
    {
        $this->channel->queue_purge($this->queueName);
    }

    public function delete(): void
    {
        $this->channel->queue_delete($this->queueName);
    }

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
        } catch (AMQPTimeoutException $ex) {
            $consumer->timeOut();
        }

        $this->channel->basic_cancel($consumerTag);
    }

    public function ack(AMQPMessage $message): void
    {
        $this->channel->basic_ack($message->getDeliveryTag());
        $this->logDebug('message_ack', $message->getBody(), 'ACK-ing message');
    }

    public function reject(AMQPMessage $message): void
    {
        $this->channel->basic_reject($message->getDeliveryTag(), true);
        $this->logDebug('message_rejected', $message->getBody(), 'Rejecting message');
    }

    public function discard(AMQPMessage $message): void
    {
        $this->channel->basic_reject($message->getDeliveryTag(), false);
        $this->logDebug('message_discarded', $message->getBody(), 'Discarding message');
    }

    public function publish(AMQPMessage $message)
    {
        $this->channel->basic_publish($message, '', $this->queueName);
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
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
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
