<?php

include_once __DIR__ . '/phpClickHouse-master/include.php';
include_once __DIR__ . '/phpClickHouse-master/src/Client.php';

//use ClickHouseDB\Client;
use ClickHouseDB\Exception\DatabaseException;
use ClickHouseDB\Exception\QueryException;

class DataToTransfer
{
    public $processed_dir = 'processed';

    public function __construct()
    {
        $this->config = include_once __DIR__ . '/inc/CH_db_params.inc';
        try {
            $this->db = new ClickHouseDB\Client($this->config);
            $this->db->enableHttpCompression();
        } catch (DatabaseException $err) {
            die('Failed connection to Clickhouse. Error message: ' . $err . PHP_EOL);
        }
    }


    /**
     * create main table
     */
    public function create_table_MeteostationsData()
    {
        echo 'Creating tables...'.PHP_EOL;
        $this->db->write('DROP TABLE IF EXISTS MeteostationsData');
        $this->db->write('
            CREATE TABLE IF NOT EXISTS MeteostationsData (
                date DATE, 
                meteostationID String,
                timestamp DateTime,
                temperature Float64,
                pressure Float64,
                humidity Float64,
                wind_max_ms Float64,
                wind_min_ms Float64,
                wind_avg Float64,
                wind_direction Float64
            ) ENGINE = ReplacingMergeTree(date, (meteostationID, timestamp), 8192)
            
        ');
    }

    /*
     * create temporary table in CH with Log engine (w/o Date column)
     */
    public function create_table_LogMeteostationsData()
    {
        $this->db->write('DROP TABLE IF EXISTS LogMeteostationsData');
        $this->db->write('
            CREATE TABLE IF NOT EXISTS LogMeteostationsData (
                meteostationID String,
                timestamp DateTime,
                temperature Float64,
                pressure Float64,
                humidity Float64,
                wind_max_ms Float64,
                wind_min_ms Float64,
                wind_avg Float64,
                wind_direction Float64
            ) ENGINE = Log()
        ');
    }


    /**
     * insert a file to Log table
     * insert data from Log table to main table with Date field
     */
    private function insert_file_to_clickhouse($file_name)
    {
        try {
            $this->create_table_LogMeteostationsData();
            echo 'Start inserting file' . $file_name . ' to CH' . PHP_EOL;
            $this->db->insertBatchFiles('LogMeteostationsData', [$file_name], [
                'meteostationID',
                'timestamp',
                'temperature',
                'pressure',
                'humidity',
                'wind_max_ms',
                'wind_min_ms',
                'wind_avg',
                'wind_direction'
            ]);
            $this->db->write('INSERT INTO MeteostationsData  SELECT CAST(timestamp AS Date) AS date, * FROM LogMeteostationsData');
            echo "File " . $file_name . " is loaded to Clickhouse." . PHP_EOL;

            // move file to 'processed' folder

            if (!is_dir($this->processed_dir)) {
                chdir('.');
                mkdir($this->processed_dir);
            }
            rename($file_name, $this->processed_dir . DIRECTORY_SEPARATOR . $file_name);

        } catch (QueryException $err) {
            echo 'Failed insert to CH. File ' . $file_name . ' Error message: ' . $err . PHP_EOL;
        } finally {
            $this->db->write('DROP TABLE IF EXISTS LogMeteostationsData');
        }
    }

    /**
     * read all files from a directory and call insert_file_to_clickhouse for each file
     */
    public function process_files_list($csv_dir)
    {
        chdir($csv_dir);
        $list_of_files = scandir('.');
        if (!$list_of_files) {
            echo "Nothing to process in " . $csv_dir . PHP_EOL;
        }

        foreach ($list_of_files as $file_name) {
            if (is_file($file_name)) {
                $this->insert_file_to_clickhouse($file_name);
            }
        }
        echo 'Folder ' . $csv_dir . ' is empty, no files to process...' . PHP_EOL;
        $sum = $this->db->select('SELECT COUNT() as counter FROM MeteostationsData')->rows()[0]['counter'];
        echo 'Total rows added is ' . $sum . PHP_EOL;
        }
}
if (!$argv[1]) {
    echo "Please input dir name with files to processed...";
}
else {
    $dir_to_process = $argv[1];
    $t = microtime(true);
    $data = new DataToTransfer();
    $data->create_table_MeteostationsData();
    $data->process_files_list($dir_to_process);
    echo 'Used time = '.round(microtime(true)-$t, 2).PHP_EOL;

}






