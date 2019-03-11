<?php
/*
 * NOTE! to use "INTO OUTFILE" statement it's necessary to enable it in MySQL server config: add to my.cnf
 * or other active config file following parameter:
 * secure_file_priv = ""
 *
 */

namespace MSConnection;

use PDO;
use PDOException;

class MySQL_conn
{
    public function __construct()
    {

        $this->config = include_once __DIR__ . '/inc/mysql_db_params.inc';
        //$dsn = "mysql:host=$this->config['host'];dbname=".$this->config['dbname'];
        $dsn = 'mysql:host=localhost;dbname=meteo_db';
        $options = [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES   => false,
        ];

        try {

            $this->conn = new PDO($dsn, $this->config['username'], $this->config['password'], $options);

        } catch (PDOException $err) {
            die('Failed connection to MySQL. Error message: ' . $err->getMessage().PHP_EOL);
        }
    }
}
