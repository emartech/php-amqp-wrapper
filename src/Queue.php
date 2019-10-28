<?php

namespace Emartech\AmqpWrapper;

use ErrorException;
use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Throwable;

class Queue
{
    private $logger;
    private $channel;
    private $queueName;
    private $timeout;
    private $messageBuffer;
    private $batchSize = 2;

    public static function create(string $queueName, AMQPChannel $channel, int $timeout, int $batchSize, LoggerInterface $logger): self
    {
        return new self($queueName, $channel, $timeout, $batchSize, new MessageBuffer($channel), $logger);
    }

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
        $consumedCount = 0;
        $rejectedCount = 0;
        foreach ($this->messageBuffer->getMessages() as $message) {
            try {
                $messageBody = $message->getContents();
                $consumer->consume($messageBody);
                $message->ack();
                $consumedCount++;
                $this->logDebug('message_ack', $message->getRawBody(), 'ACK-ing message');
            } catch (Throwable $t) {
                $this->logError('consume_failure', $message->getRawBody(), $t);
                $message->requeue();
                $rejectedCount++;
                $this->logDebug('message_reject', $message->getRawBody(), 'rejecting message');
            }
        }
        $this->logInfo('consume_success', 'messages consumed', [
            'messages_consumed_count' => $consumedCount,
            'messages_rejected_count' => $rejectedCount,
        ]);

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
