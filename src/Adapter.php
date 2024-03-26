<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Db;

use InvalidArgumentException;
use Yiisoft\Queue\Adapter\AdapterInterface;
use Yiisoft\Queue\Enum\JobStatus;
use Yiisoft\Queue\Message\MessageInterface;
use Yiisoft\Queue\QueueFactory;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Query\Query;

final class Adapter implements AdapterInterface
{
    /**
     * @var int timeout
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
        private string $channel = QueueFactory::DEFAULT_CHANNEL_NAME,
    ) {
    }

    public function runExisting(callable $handlerCallback): void
    {        
        $result = true;
        while (($payload = $this->reserve()) && ($result === true)) {
            if ($result = $handlerCallback(\unserialize($payload['job']))) {
                $this->release($payload);
            }
        }
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
        $this->runExisting($handlerCallback);
    }

    public function withChannel(string $channel): self
    {
        if ($channel === $this->channel) {
            return $this;
        }

        $new = clone $this;
        $new->channel = $channel;

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
        // TWK TODO ??? return $this->db->useMaster(function () {
        // TWK TODO ??? if (!$this->mutex->acquire(__CLASS__ . $this->channel, $this->mutexTimeout)) {
        // TWK TODO ??? throw new \Exception('Has not waited the lock.');
        // TWK TODO ??? }
            
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
                // TWK TODO ??? $this->mutex->release(__CLASS__ . $this->channel);
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
    
}
