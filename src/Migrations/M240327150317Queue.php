<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Db\Migration;

use Yiisoft\Db\Migration\MigrationBuilder;
use Yiisoft\Db\Migration\RevertibleMigrationInterface;
use Yiisoft\Db\Migration\TransactionalMigrationInterface;

final class M240327150317Queue implements RevertibleMigrationInterface, TransactionalMigrationInterface
{
    public $tableName = '{{%queue}}';

    /*
    CREATE TABLE `queue` (
      `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
      `channel` varchar(255) NOT NULL,
      `job` blob NOT NULL,
      `pushed_at` int NOT NULL,
      `ttr` int NOT NULL,
      `delay` int NOT NULL DEFAULT '0',
      `priority` int UNSIGNED NOT NULL DEFAULT '1024',
      `reserved_at` int DEFAULT NULL,
      `attempt` int DEFAULT NULL,
      `done_at` int DEFAULT NULL,
      KEY `channel` (`channel`),
      KEY `priority` (`priority`),
      KEY `reserved_at` (`reserved_at`)
    )
     */
    public function up(MigrationBuilder $b): void
    {
        $b->createTable($this->tableName, [
            'id' => $b->primaryKey(),
            'channel' => $b->string()->notNull(),
            'job' => $b->binary()->notNull(),
            'pushed_at' => $b->integer()->notNull(),
            'ttr' => $b->integer()->notNull(),
            'delay' => $b->integer()->notNull()->defaultValue(0),
            'priority' => $b->integer()->unsigned()->notNull()->defaultValue(1024),
            'reserved_at' => $b->integer(),
            'attempt' => $b->integer(),
            'done_at' => $b->integer(),
        ]);
        $b->createIndex($this->tableName, 'channel', 'channel');
        $b->createIndex($this->tableName, 'priority', 'priority');
        $b->createIndex($this->tableName, 'reserved_at', 'reserved_at');
    }
    
    public function down(MigrationBuilder $b): void
    {
        $b->dropTable($this->tableName);
    }
}
