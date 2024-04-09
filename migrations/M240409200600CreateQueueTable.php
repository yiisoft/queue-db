<?php

declare(strict_types=1);

use Yiisoft\Db\Migration\MigrationBuilder;
use Yiisoft\Db\Migration\RevertibleMigrationInterface;
use Yiisoft\Db\Migration\TransactionalMigrationInterface;

final class M240409200600CreateQueueTable implements RevertibleMigrationInterface, TransactionalMigrationInterface
{
    private const TABLE_PREFIX = ''; // Add table prefix if required
    private const QUEUE_TABLE = self::TABLE_PREFIX . 'queue';

    public function up(MigrationBuilder $b): void
    {
        $this->createAssignmentsTable($b);
    }

    public function down(MigrationBuilder $b): void
    {
        $b->dropTable(self::QUEUE_TABLE);
    }

    private function createAssignmentsTable(MigrationBuilder $b): void
    {
        $b->createTable(
            self::QUEUE_TABLE,
            [
                'id' => 'int NOT NULL AUTO_INCREMENT',
                'channel' => 'varchar(255) NOT NULL',
                'job' => 'blob NOT NULL',
                'pushed_at' => 'int NOT NULL',
                'ttr' => 'int NOT NULL',
                'delay' => 'int NOT NULL DEFAULT 0',
                'priority' => 'int UNSIGNED NOT NULL DEFAULT 1024',
                'reserved_at' => 'int DEFAULT NULL',
                'attempt' => 'int DEFAULT NULL',
                'done_at' => 'int DEFAULT NULL',
                'PRIMARY KEY ([[id]])',
                'KEY channel ([[channel]])',
                'KEY reserved_at ([[reserved_at]])',
                'KEY priority ([[priority]])',
            ],
        );
    }
}
