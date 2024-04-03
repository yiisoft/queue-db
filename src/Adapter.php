<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Db;

use InvalidArgumentException;
use Yiisoft\Queue\Adapter\AdapterInterface;
use Yiisoft\Queue\Cli\LoopInterface;
use Yiisoft\Queue\Enum\JobStatus;
use Yiisoft\Queue\Message\MessageInterface;
use Yiisoft\Queue\QueueFactory;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Query\Query;
use Yiisoft\Mutex\MutexFactoryInterface;
use Yiisoft\Mutex\MutexInterface;

final class Adapter implements AdapterInterface
{
    /**
     * @var MutexInterface Mutex interface.
     */
    public MutexInterface $mutex;
    /**
     * @var int mutex timeout
     */
    public $mutexTimeout = 3;
    /**
     * @var string table name
     */
    public $tableName = '{{%queue}}';
    /**
     * @var bool ability to delete released messages from table
     */
    public $deleteReleased = true;
    
    
    public function __construct(
        private ConnectionInterface $db,
        private LoopInterface $loop,
        private MutexFactoryInterface $mutexFactory,
        private string $channel = QueueFactory::DEFAULT_CHANNEL_NAME,
    ) {
        $this->mutex = $this->mutexFactory->create(__CLASS__ . $this->channel);
    }

    public function runExisting(callable $handlerCallback): void
    {        
        $this->run($handlerCallback, false);
    }

    public function status(string|int $id): JobStatus
    {
        $id = (int) $id;
        
        $payload = (new Query($this->db))
        ->from($this->tableName)
        ->where(['id' => $id])
        ->one();
        
        if (!$payload) {
            if ($this->deleteReleased) {
                return JobStatus::done();
            }
            
            throw new InvalidArgumentException("Unknown message ID: $id.");
        }
        
        if (!$payload['reserved_at']) {
            return JobStatus::waiting();
        }
        
        if (!$payload['done_at']) {
            return JobStatus::reserved();
        }
        
        return JobStatus::done();
    }

    public function push(MessageInterface $message): MessageInterface
    {
        $metadata = $message->getMetadata();
        $this->db->createCommand()->insert($this->tableName, [
            'channel' => $this->channel,
            'job' => \serialize($message),
            'pushed_at' => time(),
            'ttr' => $metadata['ttr'] ?? 300,
            'delay' => $metadata['delay'] ?? 0,
            'priority' => $metadata['priority'] ?? 1024,
        ])->execute();
        $tableSchema = $this->db->getTableSchema($this->tableName);
        $key = $tableSchema ? $this->db->getLastInsertID($tableSchema->getSequenceName()) : $tableSchema;
        
        return new IdEnvelope($message, $key);
    }

    public function subscribe(callable $handlerCallback): void
    {
        $this->run($handlerCallback, true, 5); // TWK TODO timeout should not be hard coded
    }

    public function withChannel(string $channel): self
    {
        if ($channel === $this->channel) {
            return $this;
        }

        $new = clone $this;
        $new->channel = $channel;
        $new->mutex = $this->mutexFactory->create(__CLASS__ . $this->channel);
        
        return $new;
    }
    
    /**
     * Takes one message from waiting list and reserves it for handling.
     *
     * @return array|null payload
     * @throws \Exception in case it hasn't waited the lock
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
                if (is_array($payload)) {
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
                if ($handlerCallback(\unserialize($payload['job']))) {
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
