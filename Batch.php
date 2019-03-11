<?php
/**
 * Data batch functionality
 */

namespace Batch;

include_once __DIR__ . '/my_include.php';

use CHConnection\CH_conn;
use MSConnection\MySQL_conn;
use ClickHouseDB\Exception\DatabaseException;
use PDOException;

class Batch
{
    public $start_ts;
    public $stop_ts;
    public $size; 

    public function __construct(MySQL_conn $ms_conn, CH_conn $ch_conn)
    {
        $this->ms_conn = $ms_conn;
        $this->ch_conn = $ch_conn;
    }

    /*
     * select data from MySQL
     * returns array of rows (TODO needs to assure)
     */
    public function select_MS() : array
    {
        $sql = '
                SELECT meteostationID,
        timestamp,
        IFNULL(temperature,0) as temperature,
        IFNULL(pressure,0) as pressure,
        IFNULL(humidity,0) as humidity,
        IFNULL(wind_max_ms,0) as wind_max_ms,
        IFNULL(wind_min_ms,0) as wind_min_ms,
        IFNULL(wind_avg_ms,0) as wind_avg_ms,
        IFNULL(wind_direction,0) as wind_direction, 
        DATE_FORMAT(FROM_UNIXTIME(timestamp), \'%Y-%m-%d\') as date 
        FROM '.$this->ms_conn->config['tablename'].
        ' WHERE timestamp >= :start_ts AND timestamp < :stop_ts 
        ORDER BY timestamp';

        $result = $this->ms_conn->conn->prepare($sql);
        $result->execute(array(
           ':start_ts' => $this->start_ts,
            ':stop_ts' => $this->stop_ts));
        $this->size = $result->rowCount();
        return($result->fetchAll());
    }

    /*
     * insert a given array to Clickhouse
     */
    public function insert_CH(array $rows)
    {
         $this->ch_conn->conn->insertAssocBulk(
            'MeteostationsData',
            $rows
        );
   }

    /*
     * delete data from MySQL (based on timestamps)
     */
    public function delete_from_mysql()
    {
        $sql = '
                DELETE  
        FROM '.$this->ms_conn->config['tablename'].
            ' WHERE timestamp >= :start_ts AND timestamp < :stop_ts';
        $result = $this->ms_conn->conn->prepare($sql);
        $result->execute(array(
            ':start_ts' => $this->start_ts,
            ':stop_ts' => $this->stop_ts));
        $this->size = $result->rowCount();
        return($this->size);
    }

    /*
     * check if size of select from MySQL equals size of insert to Clickhouse
     */
    public function check() {
        $sql = 'SELECT COUNT() as counter 
                FROM '.$this->ms_conn->config["tablename"].
                ' WHERE (timestamp >= :start_ts) AND (timestamp <= :stop_ts)';
        $ch_size = $this->ch_conn->conn->select($sql,
            array('start_ts' => $this->start_ts,
                'stop_ts' => $this->stop_ts)
        )->rows()[0]['counter'];
        if ($ch_size==$this->size) {
            return true;
        }
        echo 'Check is NOT OK, rows selected '.$this->size.'.  Rows inserted '.$ch_size.'. start_ts='.$this->start_ts.' stop_ts='.$this->stop_ts.PHP_EOL;
        return false;
    }

    /*
     * process a batch, catch exceptions, delete batch if check is OK
     */
    public function process($start_ts, $stop_ts) : int
    {
        $this->start_ts = $start_ts;
        $this->stop_ts = $stop_ts;

        try {
            $select_result = $this->select_MS();
            $this->insert_CH($select_result);
        }
        catch (PDOException $err) {
            echo 'MySQL select is failed: '.$err->getMessage().PHP_EOL;
        }
        catch (DatabaseException $err) {
            echo 'ClickHouse insert is failed: '.$err->getMessage().PHP_EOL;
        }
        finally {
            $check = $this->check();
            //if ($check) {
            //        echo 'Check is true'.PHP_EOL;                
                   // $this->delete_from_mysql();
            //} 
            return $this->size;
        }
    }
}
