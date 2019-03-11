<?php

// mysql db parameters
return [
    'host' => "localhost",
    'username' => "meteo_user",
    'password' => "meteo_Pass_42",
    'dbname' => "meteo_db",
    'charset' => "utf8",
    'tablename' => "MeteostationsData",
    'fields' => array(
        "meteostationID",
        "timestamp",
        "temperature",
        "pressure",
        "humidity",
        "wind_max_ms",
        "wind_min_ms",
        "wind_avg_ms",
        "wind_direction"
),
    'mysql_db_folder' => '/var/lib/mysql/meteo_db/',
    'hours_in_one_file' => 10000,  // 1000 for test; for prod - 5 hours period ~= 600 000 records in db
//    'dir_to_process' => '/var/lib/mysql/meteo_db/', // csv files are saved at /var/lib/mysql/meteo_db by default
    'storage_period_days' => 0
];
