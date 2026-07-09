<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Db\Tests;

use PHPUnit\Framework\TestCase;
use Yiisoft\Db\Command\CommandInterface;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Schema\TableSchemaInterface;
use Yiisoft\Mutex\MutexFactoryInterface;
use Yiisoft\Mutex\MutexInterface;
use Yiisoft\Queue\Cli\LoopInterface;
use Yiisoft\Queue\Db\Adapter;
use Yiisoft\Queue\Message\DelayEnvelope;
use Yiisoft\Queue\Message\GenericMessage;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Queue\Message\Serializer\MessageSerializerInterface;
use Yiisoft\Queue\Provider\QueueProviderInterface;

final class AdapterTest extends TestCase
{
    public function testPushUsesMessageMetaAndReturnsIdEnvelope(): void
    {
        $db = $this->createMock(ConnectionInterface::class);
        $command = $this->createMock(CommandInterface::class);
        $serializer = $this->createMock(MessageSerializerInterface::class);
        $tableSchema = $this->createMock(TableSchemaInterface::class);

        $message = new DelayEnvelope(
            (new GenericMessage('test', 'payload'))->withMeta(['ttr' => 10, 'priority' => 20]),
            5,
        );

        $serializer
            ->expects(self::once())
            ->method('serialize')
            ->with($message)
            ->willReturn('serialized-message');

        $db
            ->expects(self::once())
            ->method('createCommand')
            ->willReturn($command);

        $command
            ->expects(self::once())
            ->method('insert')
            ->with('{{%queue}}', self::callback(static function (array $row): bool {
                return $row['channel'] === QueueProviderInterface::DEFAULT_QUEUE
                    && $row['job'] === 'serialized-message'
                    && $row['ttr'] === 10
                    && $row['delay'] === 5.0
                    && $row['priority'] === 20;
            }))
            ->willReturnSelf();

        $command
            ->expects(self::once())
            ->method('execute')
            ->willReturn(1);

        $db
            ->expects(self::once())
            ->method('getTableSchema')
            ->with('{{%queue}}')
            ->willReturn($tableSchema);

        $tableSchema
            ->expects(self::once())
            ->method('getSequenceName')
            ->willReturn('queue_id_seq');

        $db
            ->expects(self::once())
            ->method('getLastInsertID')
            ->with('queue_id_seq')
            ->willReturn('42');

        $result = $this->createAdapter($db, $serializer)->push($message);

        self::assertSame('42', IdEnvelope::fromMessage($result)->getId());
    }

    public function testWithChannelClonesAdapterAndUsesNewChannelMutex(): void
    {
        $mutexFactory = $this->createMock(MutexFactoryInterface::class);
        $mutexNames = [];
        $mutexFactory
            ->expects(self::exactly(2))
            ->method('create')
            ->with(self::callback(static function (string $name) use (&$mutexNames): bool {
                $mutexNames[] = $name;
                return in_array($name, [Adapter::class . QueueProviderInterface::DEFAULT_QUEUE, Adapter::class . 'mail'], true);
            }))
            ->willReturn($this->createMock(MutexInterface::class));

        $adapter = $this->createAdapter(
            mutexFactory: $mutexFactory,
        );

        $new = $adapter->withChannel('mail');

        self::assertNotSame($adapter, $new);
        self::assertSame([Adapter::class . QueueProviderInterface::DEFAULT_QUEUE, Adapter::class . 'mail'], $mutexNames);
    }

    private function createAdapter(
        ?ConnectionInterface $db = null,
        ?MessageSerializerInterface $serializer = null,
        ?MutexFactoryInterface $mutexFactory = null,
    ): Adapter {
        if ($mutexFactory === null) {
            $mutexFactory = $this->createMock(MutexFactoryInterface::class);
            $mutexFactory
                ->method('create')
                ->willReturn($this->createMock(MutexInterface::class));
        }

        return new Adapter(
            $db ?? $this->createMock(ConnectionInterface::class),
            $serializer ?? $this->createMock(MessageSerializerInterface::class),
            $this->createMock(LoopInterface::class),
            $mutexFactory,
        );
    }
}
