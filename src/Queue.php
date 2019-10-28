<?php

namespace Emartech\AmqpWrapper;

use ErrorException;
use PhpAmqpLib\Channel\AMQPChannel;
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
        $channelWrapper = new Channel($channel, $logger, $queueName);
        return new self($queueName, $channelWrapper, $timeout, $batchSize, new MessageBuffer($channelWrapper, $batchSize), $logger);
    }

    public function __construct(string $queueName, Channel $channel, int $timeout, int $batchSize, MessageBuffer $messageBuffer, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->channel = $channel;
        $this->queueName = $queueName;
        $this->timeout = $timeout;
        $this->batchSize = $batchSize;
        $this->messageBuffer = $messageBuffer;
    }

    public function send(array $messageBody): void
    {
        $this->createMessage($messageBody)->publish();
    }

    public function createMessage($messageParams): Message
    {
        return new Message($this->channel, new AMQPMessage(
            json_encode($messageParams),
            [
                'content_type' => 'text/plain',
                'delivery_mode' => 2,
            ]
        ));
    }

    /**
     * @throws ErrorException
     */
    public function consume(QueueConsumer $consumer): void
    {
        $this->channel->consume($this, $consumer, $this->batchSize, $this->timeout);
    }

    public function addMessage(AMQPMessage $rawMessage)
    {
        $this->messageBuffer->addMessage($rawMessage);
    }

    public function isBufferFull()
    {
        return $this->messageBuffer->isFull();
    }

    public function processMessages(QueueConsumer $consumer, bool $force): void
    {
        if (!$force && !$this->messageBuffer->isFull()) {
            return;
        }

        $consumedCount = 0;
        $failedCount = 0;
        foreach ($this->messageBuffer->getMessages() as $message) {
            try {
                $consumer->consume($message);
                $consumedCount++;
            } catch (Throwable $t) {
                $consumer->error($message, $t);
                $failedCount++;
            }
        }

        $this->logInfo('consume_success', 'messages consumed', [
            'messages_consumed_count' => $consumedCount,
            'messages_failed_count' => $failedCount,
            'messages_processed_count' => $failedCount + $consumedCount,
        ]);

        $this->messageBuffer->flush();
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
}
