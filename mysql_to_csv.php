<?php
/*
 * NOTE! to use "INTO OUTFILE" statement it's necessary to enable it in MySQL server config: add to my.cnf
 * or other active config file following parameter:
 * secure_file_priv = ""
 *
 */


class MySQLData {

    public function __construct() {

        $this->config = include_once __DIR__.'/inc/mysql_db_params.inc';

        try {
            $this->conn = new mysqli($this->config['servername'],
                               $this->config['username'],
                               $this->config['password'],
                               $this->config['dbname']
            );
        }
        catch (mysqli_sql_exception $err) {
            die('Failed connection to MySQL. Error message ' . $err);
        }
    }

    /**
     *  make a csv file with a data portion
     */
    public function retrieve_file_from_MySQL($from_timestamp, $to_timestamp) {
        $sql = "SELECT ";
        $sql .= implode(", ", $this->config['fields']);
        $sql .= " FROM " . $this->config['tablename'];
        $sql .= " WHERE timestamp >= " . "\"" . ltrim(rtrim($from_timestamp)) . "\"";
        $sql .= " AND timestamp < " . "\"" . ltrim(rtrim($to_timestamp)) . "\"";
        $sql .= " ORDER BY timestamp";
        $sql .= " INTO OUTFILE \"". $this->config['dir_to_process'] . "data-batch-" . ltrim(rtrim($from_timestamp)) . ".csv\"";
        $sql .= " FIELDS TERMINATED BY \",\"";
        $sql .= " ENCLOSED BY \"'\"";
        $sql .= " LINES TERMINATED BY \"\\n\"";
        $result = $this->conn->query($sql);

    }

    /**
     * estimate portion size and retrieve data portion by portion
     */
    public function get() {

        $sql = 'SELECT min(timestamp) AS min_ts FROM ' . $this->config['tablename'];
        $result = $this->conn->query($sql);
        $row = $result->fetch_assoc();
        $min_timestamp = $row["min_ts"];

        $sql = 'SELECT max(timestamp) AS max_ts FROM ' . $this->config['tablename'];
        $result = $this->conn->query($sql);
        $row = $result->fetch_assoc();
        $max_timestamp = $row["max_ts"];

        $files_qty = ($max_timestamp - $min_timestamp) / ($this->config['hours_in_one_file']*3600);

        for ($i=0; $i < $files_qty; $i+=$this->config['hours_in_one_file']) {
            $from_timestamp = $min_timestamp + $i*3600;
            $to_timestamp = $from_timestamp + $this->config['hours_in_one_file']*3600;
            $this->retrieve_file_from_MySQL($from_timestamp, $to_timestamp);
        }
        echo 'Data from MySQL is loaded to csv files. Number of files loaded are '. $files_qty . PHP_EOL;
    }
}

$data = new MySQLData();
echo 'Start retrieving data from MySQL...' . PHP_EOL;
$data->get();