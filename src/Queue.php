<?php

namespace Emartech\AmqpWrapper;

use ErrorException;
use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class Queue
{
    private $logger;
    private $channel;
    private $queueName;
    private $timeout;
    private $messageBuffer;
    private $batchSize = 2;


    public function __construct(string $queueName, AMQPChannel $channel, int $timeout, int $batchSize, MessageBuffer $messageBuffer, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->channel = $channel;
        $this->queueName = $queueName;
        $this->timeout = $timeout;
        $this->batchSize = $batchSize;
        $this->messageBuffer = $messageBuffer;
    }

    /**
     * @throws ErrorException
     */
    public function consume(QueueConsumer $consumer): void
    {
        $consumerTag = 'consumer' . getmypid();
        $this->channel->basic_qos(0, $this->batchSize, false);
        $this->channel->basic_consume($this->queueName, $consumerTag, false, false, false, false, function (AMQPMessage $rawMessage) use ($consumer) {
            $this->messageBuffer->addMessage($rawMessage);
            $this->logDebug('consume_prepare', $rawMessage->body, 'Consuming message');

            if ($this->messageBuffer->getMessageCount() >= $this->batchSize) {
                $this->processMessages($consumer);
            }
        });

        try {
            do {
                $this->channel->wait(null, false, $this->timeout);
            } while (count($this->channel->callbacks));
        } catch (AMQPTimeoutException $e) {
            $this->processMessages($consumer);
        }

        $this->channel->basic_cancel($consumerTag);
    }

    private function processMessages(QueueConsumer $consumer): void
    {

        $amqpMessages = $this->messageBuffer->getMessages();
        try {
            foreach ($amqpMessages as $message) {
                $messageBody = json_decode($message->body, true);
                $consumer->consume($messageBody);
            }
            foreach ($amqpMessages as $message) {
                $this->channel->basic_ack($message->delivery_info['delivery_tag']);
                $this->logDebug('message_ack', $message->body, 'ACK-ing message');
            }
            $this->logInfo('consume_success', 'messages consumed', ['message_count' => $this->messageBuffer->getMessageCount()]);
        } catch (Exception $ex) {
            foreach ($amqpMessages as $message) {
                $this->logError('consume_failure', $message->body, $ex);
                $this->channel->basic_reject($message->delivery_info['delivery_tag'], true);
                $this->logDebug('message_reject', $message->body, 'rejecting message');
            }
        }

        $this->messageBuffer->flush();
    }

    public function send(array $messageBody): void
    {
        $message = $this->createMessage($messageBody);
        $this->channel->basic_publish($message, 'amq.direct');

        $this->logDebug('message_sent', json_encode($messageBody), 'AMQP message sent');
    }

    private function createMessage($messageParams): AMQPMessage
    {
        return new AMQPMessage(
            json_encode($messageParams),
            [
                'content_type' => 'text/plain',
                'delivery_mode' => 2,
            ]
        );
    }

    private function logError(string $event, string $rawMessage, Exception $ex): void
    {
        $this->logger->error($ex->getMessage(), [
            'queue' => $this->queueName,
            'event' => $event,
            'raw_message' => $rawMessage,
            'exception' => $ex,
        ]);
    }

    private function logInfo(string $event, string $logMessage, array $context = []): void
    {
        $this->logger->info($logMessage,
            array_merge(
                [
                    'queue' => $this->queueName,
                    'event' => $event,
                ],
                $context
            )
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
