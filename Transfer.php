<?php
/**
 * Class describes main functionality.
 * Transfers data from MySQL to ClickHouse by batches
 * At the end checks if unprocessed data in MySQL is remained
 */

include_once __DIR__ . '/my_include.php';

use MSConnection\MySQL_conn;
use CHConnection\CH_conn;
use Batch\Batch;

class Transfer
{
    public $min_timestamp;
    public $max_timestamp;
    public $ms_conn;
    public $ch_conn;

    /*
     * make connections to both db
     */
    public function __construct()
    {
        $this->ms_conn = new MySQL_conn();
        $this->ch_conn = new CH_conn();
        $this->ch_conn->create_table_MeteostationsData();

    }

    /*
     * main function
     * determine batch size, process batches in loop, print number of rows processed
     */
    public function process()
    {
        $sql_min = 'SELECT min(timestamp) AS min_ts FROM '.$this->ms_conn->config['tablename'];
        $sql_max = 'SELECT max(timestamp) AS max_ts FROM '.$this->ms_conn->config['tablename'];
        $result = $this->ms_conn->conn->prepare($sql_min);
        $result->execute();
        $this->min_timestamp = $result->fetch()['min_ts'];
       
        $result = $this->ms_conn->conn->prepare($sql_max);
        $result->execute();
        $this->max_timestamp = $result->fetch()['max_ts'];

        $this->max_timestamp = $this->max_timestamp - $this->ms_conn->config['storage_period_days']*60*60*24;

        $batches_qty = ceil(($this->max_timestamp - $this->min_timestamp) / ($this->ms_conn->config['hours_in_one_file']*60*60));
 echo 'Start processing. Min_ts = '.$this->min_timestamp.' Max_ts = '.$this->max_timestamp.' Batches number = '.$batches_qty.PHP_EOL;
              
 $batch = new Batch($this->ms_conn, $this->ch_conn);
        $total_sum = 0;
        $delta_ts = $this->ms_conn->config['hours_in_one_file']*60*60;
           
        for ($i=0; $i <= $batches_qty; $i++) { //=$this->ms_conn->config['hours_in_one_file'{ 
            $start_timestamp = $this->min_timestamp + $i*$delta_ts;
            $stop_timestamp = $start_timestamp + $delta_ts;
            $sum = $batch->process($start_timestamp, $stop_timestamp);
            $total_sum = $total_sum + $sum;
            echo 'Total rows processed: '. $total_sum . PHP_EOL;
            
        }
    }

    /*
     * check if there is unprocessed data in MySQL
     * returns the number of unprocessed rows (0 in best case)
     */
    public function check()
    {
        $sql = '
                SELECT COUNT(*) as counter 
                FROM '.$this->ms_conn->config['tablename'].
                ' WHERE timestamp >= :min_ts AND timestamp <= :max_ts';
        $stat = $this->ms_conn->conn->prepare($sql);
        $stat->execute(
            array(':max_ts' => $this->max_timestamp,
                  ':min_ts' => $this->min_timestamp));
        $size = $stat->fetch()['counter'];
       
 return $size;
    }

}

$transfer = new Transfer();
$transfer->process();
echo 'Transfer process is finished.' . PHP_EOL;
echo 'Check result: number of rows to delete in MySQL is '.$transfer->check().PHP_EOL;

