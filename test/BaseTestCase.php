<?php

namespace Test;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class BaseTestCase extends TestCase
{
    /**
     * @var MockObject|LoggerInterface
     */
    protected $dummyLogger;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dummyLogger = $this->createMock(LoggerInterface::class);
    }
}
