<?php

declare(strict_types=1);

use ShipMonk\ComposerDependencyAnalyser\Config\Configuration;

return (new Configuration())
    ->disableComposerAutoloadPathScan()
    ->setFileExtensions(['php'])
    ->addPathToScan(__DIR__ . '/src', isDev: false)
    ->addPathToScan(__DIR__ . '/migrations', isDev: false)
    ->addPathToScan(__DIR__ . '/tests', isDev: true)
    // yiisoft/db-migration is an optional integration (see composer.json "suggest"), not a hard dependency.
    ->ignoreUnknownClasses([
        'Yiisoft\Db\Migration\MigrationBuilder',
        'Yiisoft\Db\Migration\RevertibleMigrationInterface',
        'Yiisoft\Db\Migration\TransactionalMigrationInterface',
    ]);
