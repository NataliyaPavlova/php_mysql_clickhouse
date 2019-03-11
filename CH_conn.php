<?php

namespace CHConnection;

include_once __DIR__ . '/my_include.php';

use ClickHouseDB\Exception\DatabaseException;
use ClickHouseDB\Client;

class CH_conn
{
    public function __construct()
    {
        $this->config = include_once __DIR__ . '/inc/CH_db_params.inc';
        try {
            $this->conn = new Client($this->config);
            $this->conn->enableHttpCompression();
        } catch (DatabaseException $err) {
            die('Failed connection to Clickhouse. Error message: ' . $err . PHP_EOL);
        }
    }


    /**
     * create main table
     */
    public function create_table_MeteostationsData()
    {
        echo 'Creating tables...' . PHP_EOL;
        $this->conn->write('DROP TABLE IF EXISTS MeteostationsData');
        $this->conn->write('
            CREATE TABLE IF NOT EXISTS MeteostationsData (
                meteostationID String,
                timestamp DateTime,
                temperature Float64,
                pressure Float64,
                humidity Float64,
                wind_max_ms Float64,
                wind_min_ms Float64,
                wind_avg_ms Float64,
                wind_direction Float64,
                date DATE 
            ) ENGINE = MergeTree(date, (meteostationID, timestamp), 8192)
            
        ');
    }
}
