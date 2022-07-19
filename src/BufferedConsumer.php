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
    private MessageBuffer $buffer;
    private QueueConsumer $delegate;
    private LoggerInterface $logger;
    private string $queueName;


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
        $this->logInfo(['messages_consumed' => $this->buffer->getMessageCount()]);
    }

    private function logConsumeFailure(Throwable $t): void
    {
        $this->logError($t);
    }

    private function logInfo(array $context = []): void
    {
        $this->logger->info(
            'messages consumed',
            array_merge(
                [
                    'queue' => $this->queueName,
                    'event' => 'consume_success',
                ],
                $context
            )
        );
    }
    private function logError(Throwable $t): void
    {
        $this->logger->error(
            'consume failed',
            array_merge(
                [
                    'queue' => $this->queueName,
                    'event' => 'consume_failure',
                    'exception' => $t,
                ]
            )
        );
    }
}
