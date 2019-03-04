<?php

// mysql db parameters
return [
    'servername' => "localhost",
    'username' => "meteo_user",
    'password' => "meteo_Pass_42",
    'dbname' => "meteo_db",
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
    'hours_in_one_file' => 5,  // 5 hours period ~= 600 000 records in db
    'dir_to_process' => '/var/lib/mysql/meteo_db/' // csv files are saved at /var/lib/mysql/meteo_db by default
];
