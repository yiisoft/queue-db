<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Db\Tests;

use PHPUnit\Framework\TestCase;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Query\Query;
use Yiisoft\Db\Sqlite\Connection;
use Yiisoft\Db\Sqlite\Driver;
use Yiisoft\Mutex\MutexFactoryInterface;
use Yiisoft\Mutex\MutexInterface;
use Yiisoft\Queue\Cli\SimpleLoop;
use Yiisoft\Queue\Db\Adapter;
use Yiisoft\Queue\Message\GenericMessage;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Queue\Message\MessageInterface;
use Yiisoft\Queue\Message\Serializer\JsonMessageEncoder;
use Yiisoft\Queue\Message\Serializer\MessageSerializer;
use Yiisoft\Queue\MessageStatus;

use function array_slice;
use function count;

final class AdapterRunTest extends TestCase
{
    private ConnectionInterface $db;

    protected function setUp(): void
    {
        $this->db = new Connection(new Driver('sqlite::memory:'));
        $this->db->createCommand()->createTable('{{%queue}}', [
            'id' => 'integer PRIMARY KEY AUTOINCREMENT',
            'channel' => 'varchar(255) NOT NULL',
            'job' => 'blob NOT NULL',
            'pushed_at' => 'integer NOT NULL',
            'ttr' => 'integer NOT NULL',
            'delay' => 'integer NOT NULL DEFAULT 0',
            'priority' => 'integer NOT NULL DEFAULT 1024',
            'reserved_at' => 'integer DEFAULT NULL',
            'attempt' => 'integer DEFAULT NULL',
            'done_at' => 'integer DEFAULT NULL',
        ])->execute();
    }

    public function testRunExistingStopsAndReturnsMessageWhenHandlerRefuses(): void
    {
        $adapter = $this->createAdapter();
        $ids = $this->pushMessages($adapter, 5);

        $handled = $this->runExisting($adapter, 2);

        self::assertSame([1, 2], $handled);
        foreach (array_slice($ids, 2) as $id) {
            self::assertSame(MessageStatus::WAITING, $adapter->status($id));
            self::assertNull($this->getRow($id)['attempt']);
        }
    }

    public function testRunExistingContinuesWithRefusedMessageOnNextRun(): void
    {
        $adapter = $this->createAdapter();
        $this->pushMessages($adapter, 5);

        $this->runExisting($adapter, 2);
        $handled = $this->runExisting($adapter, 2);

        self::assertSame([3, 4], $handled);
    }

    public function testRunExistingKeepsAttemptOfRetriedMessageWhenHandlerRefuses(): void
    {
        $adapter = $this->createAdapter();
        [$id] = $this->pushMessages($adapter, 1);
        $this->db->createCommand()->update('{{%queue}}', ['attempt' => 2], ['id' => $id])->execute();

        $this->runExisting($adapter, 0);

        $row = $this->getRow($id);
        self::assertNull($row['reserved_at']);
        self::assertSame(2, (int) $row['attempt']);
    }

    public function testSubscribeStopsAndReturnsMessageWhenHandlerRefuses(): void
    {
        $adapter = $this->createAdapter();
        [$id] = $this->pushMessages($adapter, 1);

        $adapter->subscribe(static fn(): bool => false);

        self::assertSame(MessageStatus::WAITING, $adapter->status($id));
        self::assertNull($this->getRow($id)['attempt']);
    }

    public function testRunExistingReleasesAcceptedMessages(): void
    {
        $adapter = $this->createAdapter();
        $ids = $this->pushMessages($adapter, 2);

        $handled = $this->runExisting($adapter, 2);

        self::assertSame([1, 2], $handled);
        self::assertNull($this->getRow($ids[0]));
        self::assertNull($this->getRow($ids[1]));
    }

    /**
     * @return list<int|string>
     */
    private function pushMessages(Adapter $adapter, int $count): array
    {
        $ids = [];
        for ($i = 1; $i <= $count; $i++) {
            $ids[] = IdEnvelope::fromMessage($adapter->push(new GenericMessage('job', $i)))->getId();
        }

        return $ids;
    }

    /**
     * Runs existing messages, accepting at most `$limit` of them and refusing the next one.
     *
     * @return list<mixed> Payloads of the accepted messages.
     */
    private function runExisting(Adapter $adapter, int $limit): array
    {
        $handled = [];
        $adapter->runExisting(static function (MessageInterface $message) use (&$handled, $limit): bool {
            if (count($handled) >= $limit) {
                return false;
            }
            $handled[] = $message->getPayload();
            return true;
        });

        return $handled;
    }

    private function getRow(int|string $id): ?array
    {
        return (new Query($this->db))->from('{{%queue}}')->where(['id' => $id])->one();
    }

    private function createAdapter(): Adapter
    {
        $mutex = $this->createMock(MutexInterface::class);
        $mutex->method('acquire')->willReturn(true);
        $mutexFactory = $this->createMock(MutexFactoryInterface::class);
        $mutexFactory->method('create')->willReturn($mutex);

        return new Adapter(
            $this->db,
            new MessageSerializer(new JsonMessageEncoder()),
            new SimpleLoop(),
            $mutexFactory,
        );
    }
}
