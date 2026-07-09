<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Db;

use BackedEnum;
use InvalidArgumentException;
use RuntimeException;
use Yiisoft\Queue\Adapter\AdapterInterface;
use Yiisoft\Queue\Cli\LoopInterface;
use Yiisoft\Queue\Message\DelayEnvelope;
use Yiisoft\Queue\Message\MessageInterface;
use Yiisoft\Queue\Message\Serializer\MessageSerializerInterface;
use Yiisoft\Queue\MessageStatus;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Query\Query;
use Yiisoft\Mutex\MutexFactoryInterface;
use Yiisoft\Mutex\MutexInterface;
use Yiisoft\Queue\Provider\QueueProviderInterface;

final class Adapter implements AdapterInterface
{
    /**
     * @var MutexInterface Mutex interface.
     */
    public MutexInterface $mutex;
    /**
     * @var int Mutex timeout.
     */
    public $mutexTimeout = 3;
    /**
     * @var string Table name.
     */
    public $tableName = '{{%queue}}';
    /**
     * @var bool Ability to delete released messages from table.
     */
    public $deleteReleased = true;

    public function __construct(
        private ConnectionInterface $db,
        private MessageSerializerInterface $serializer,
        private LoopInterface $loop,
        private MutexFactoryInterface $mutexFactory,
        private string $channel = QueueProviderInterface::DEFAULT_QUEUE,
    ) {
        $this->mutex = $this->mutexFactory->create(self::class . $this->channel);
    }

    public function runExisting(callable $handlerCallback): void
    {
        $this->run($handlerCallback, false);
    }

    public function status(string|int $id): MessageStatus
    {
        $id = (int) $id;

        $payload = (new Query($this->db))
        ->from($this->tableName)
        ->where(['id' => $id])
        ->one();

        if ($payload === null) {
            if ($this->deleteReleased) {
                return MessageStatus::DONE;
            }

            throw new InvalidArgumentException("Unknown message ID: $id.");
        }

        if (!is_array($payload)) {
            throw new RuntimeException('Queue payload must be an array.');
        }

        if (!$payload['reserved_at']) {
            return MessageStatus::WAITING;
        }

        if (!$payload['done_at']) {
            return MessageStatus::RESERVED;
        }

        return MessageStatus::DONE;
    }

    public function push(MessageInterface $message): MessageInterface
    {
        $meta = $message->getMeta();
        $this->db->createCommand()->insert($this->tableName, [
            'channel' => $this->channel,
            'job' => $this->serializer->serialize($message),
            'pushed_at' => time(),
            'ttr' => $meta['ttr'] ?? 300,
            'delay' => $meta[DelayEnvelope::META_DELAY_SECONDS] ?? 0,
            'priority' => $meta['priority'] ?? 1024,
        ])->execute();
        $tableSchema = $this->db->getTableSchema($this->tableName);
        $key = $tableSchema ? $this->db->getLastInsertID($tableSchema->getSequenceName()) : $tableSchema;

        return new IdEnvelope($message, $key);
    }

    public function subscribe(callable $handlerCallback): void
    {
        $this->run($handlerCallback, true, 5); // TWK TODO timeout should not be hard coded
    }

    public function withChannel(BackedEnum|string $channel): self
    {
        $channel = is_string($channel) ? $channel : (string) $channel->value;

        if ($channel === $this->channel) {
            return $this;
        }

        $new = clone $this;
        $new->channel = $channel;
        $new->mutex = $this->mutexFactory->create(self::class . $new->channel);

        return $new;
    }

    /**
     * Takes one message from waiting list and reserves it for handling.
     *
     * @throws \Exception in case it hasn't waited the lock
     * @return array|null payload
     */
    protected function reserve(): array|null
    {
        // TWK TODO what is useMaster in Yii3 return $this->db->useMaster(function () {
        if (!$this->mutex->acquire($this->mutexTimeout)) {
            throw new \Exception('Has not waited the lock.');
        }

        try {
            $this->moveExpired();

            // Reserve one message
            $payload = (new Query($this->db))
            ->from($this->tableName)
            ->andWhere(['channel' => $this->channel, 'reserved_at' => null])
            ->andWhere('[[pushed_at]] <= :time - [[delay]]', [':time' => time()])
            ->orderBy(['priority' => SORT_ASC, 'id' => SORT_ASC])
            ->limit(1)
            ->one();
            if ($payload !== null && !is_array($payload)) {
                throw new RuntimeException('Queue payload must be an array.');
            }

            if ($payload !== null) {
                $payload['reserved_at'] = time();
                $payload['attempt'] = (int) $payload['attempt'] + 1;
                $this->db->createCommand()->update($this->tableName, [
                    'reserved_at' => $payload['reserved_at'],
                    'attempt' => $payload['attempt'],
                ], [
                    'id' => $payload['id'],
                ])->execute();

                // pgsql
                if (is_resource($payload['job'])) {
                    $payload['job'] = stream_get_contents($payload['job']);
                }
            }
        } finally {
            $this->mutex->release();
        }

        return $payload;
        // TWK TODO ??? });
    }

    /**
     * @param array $payload
     */
    protected function release($payload): void
    {
        if ($this->deleteReleased) {
            $this->db->createCommand()->delete(
                $this->tableName,
                ['id' => $payload['id']]
            )->execute();
        } else {
            $this->db->createCommand()->update(
                $this->tableName,
                ['done_at' => time()],
                ['id' => $payload['id']]
            )->execute();
        }
    }

    /**
     * Moves expired messages into waiting list.
     */
    private function moveExpired(): void
    {
        if ($this->reserveTime !== time()) {
            $this->reserveTime = time();
            $this->db->createCommand()->update(
                $this->tableName,
                ['reserved_at' => null],
                '[[reserved_at]] < :time - [[ttr]] and [[reserved_at]] is not null and [[done_at]] is null',
                null,
                [':time' => $this->reserveTime]
            )->execute();
        }
    }

    /**
     * @var int reserve time
     */
    private $reserveTime = 0;

    /**
     * Listens queue and runs each job.
     *
     * @param callable(MessageInterface): bool  $handlerCallback The handler which will handle messages. Returns false if it cannot continue handling messages
     * @param bool $repeat whether to continue listening when queue is empty.
     * @param non-negative-int $timeout number of seconds to sleep before next iteration.
     */
    public function run(callable $handlerCallback, bool $repeat, int $timeout = 0): void
    {
        while ($this->loop->canContinue()) {
            if ($payload = $this->reserve()) {
                if ($handlerCallback($this->serializer->unserialize($payload['job']))) {
                    $this->release($payload);
                }
                continue;
            }
            if (!$repeat) {
                break;
            }
            if ($timeout > 0) {
                sleep($timeout);
            }
        }
    }
}
