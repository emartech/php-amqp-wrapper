<?php

namespace Emartech\AmqpWrapper;

use ErrorException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class Channel
{
    private $channel;
    private $logger;
    private $queueName;


    public function __construct(AMQPChannel $channel, LoggerInterface $logger, string $queueName)
    {
        $this->channel = $channel;
        $this->logger = $logger;
        $this->queueName = $queueName;
    }

    public function ack(AMQPMessage $message): void
    {
        $this->channel->basic_ack($message->delivery_info['delivery_tag']);
        $this->logDebug('message_ack', $message->getBody(), 'ACK-ing message');
    }

    public function requeue(AMQPMessage $message): void
    {
        $this->channel->basic_reject($message->delivery_info['delivery_tag'], true);
        $this->logDebug('message_requeued', $message->getBody(), 'Requeueing message');
    }

    public function publish(AMQPMessage $message)
    {
        $this->channel->basic_publish($message, 'amq.direct');
        $this->logDebug('message_sent', $message->getBody(), 'AMQP message sent');
    }

    /**
     * @param Queue $queue
     * @param QueueConsumer $consumer
     * @param int $batchSize
     * @param int $timeout
     * @throws ErrorException
     */
    public function consume(Queue $queue, QueueConsumer $consumer, int $batchSize, int $timeout)
    {
        $consumerTag = 'consumer' . getmypid();
        $this->channel->basic_qos(0, $batchSize, false);
        $this->channel->basic_consume($this->queueName, $consumerTag, false, false, false, false, function (AMQPMessage $rawMessage) use ($queue, $consumer) {
            $this->logDebug('consume_prepare', $rawMessage->getBody(), 'Consuming message');
            $queue->addMessage($rawMessage);
            $queue->processMessages($consumer, false);
        });

        try {
            do {
                $this->channel->wait(null, false, $timeout);
            } while ($this->channel->is_consuming());
        } catch (AMQPTimeoutException $e) {
            $queue->processMessages($consumer, true);
        }

        $this->channel->basic_cancel($consumerTag);
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
