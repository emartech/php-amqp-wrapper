<?php

namespace Emartech\AmqpWrapper;

use Exception;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * This consumer encapsulates the former behavior of the wrapper library
 */
class BufferedConsumer implements QueueConsumer
{
    /**
     * @var MessageBuffer
     */
    private $buffer;

    /**
     * @var QueueConsumer
     */
    private $delegate;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var string
     */
    private $queueName;


    public function __construct(MessageBuffer $buffer, QueueConsumer $delegate, LoggerInterface $logger, string $queueName)
    {
        $this->buffer = $buffer;
        $this->delegate = $delegate;
        $this->logger = $logger;
        $this->queueName = $queueName;
    }

    public function getPrefetchCount(): int
    {
        return $this->buffer->getSize();
    }

    /**
     * @throws Exception
     */
    public function consume(Message $message): void
    {
        $this->buffer->addMessage($message);
        if ($this->buffer->isFull()) {
            $this->processMessages();
        }
    }

    public function timeOut(): void
    {
        $this->processMessages();
    }

    private function processMessages(): void
    {
        $messages = $this->buffer->getMessages();
        try {
            foreach ($messages as $message) {
                $this->delegate->consume($message);
            }
            foreach ($messages as $message) {
                $message->ack();
            }
            $this->logConsumeSuccess();
        } catch (Throwable $t) {
            $this->logConsumeFailure($t);
            foreach ($messages as $message) {
                $message->reject();
            }
        }
        $this->buffer->flush();
    }

    private function logConsumeSuccess(): void
    {
        $this->logInfo('consume_success', 'messages consumed', ['messages_consumed' => $this->buffer->getMessageCount()]);
    }

    private function logConsumeFailure(Throwable $t): void
    {
        $this->logError('consume_failure', 'consume failed', $t);
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
    private function logError(string $event, string $logMessage, Throwable $t, array $context = []): void
    {
        $this->logger->error($logMessage,
            array_merge(
                [
                    'queue' => $this->queueName,
                    'event' => $event,
                    'exception' => $t,
                ],
                $context
            )
        );
    }
}