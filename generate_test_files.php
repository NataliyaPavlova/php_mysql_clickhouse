<?php


{
class Tests
{
    public function generate_files_list($dir_to_processed)
    {
        if (!file_exists($dir_to_processed)) {
            mkdir($dir_to_processed);
        }

        $file_data_names = [
            'tmp/test.1.csv',
            'tmp/test.2.csv',
            'tmp/test.3.csv'
        //    'tmp/test.4.sorted.csv',
        //    'tmp/test.5.sorted.csv',
        ];
        $c = 500;
        foreach ($file_data_names as $file_name) {
            $c+=1000;
            $this->makeSomeDataFileBig($file_name, 50 * $c);
        }
        return $file_data_names;
    }


    /**
     * @param $file_name
     * @param int $size
     * @return bool
     */
    public function makeSomeDataFileBig($file_name, $size = 100, $shift = 0)
    {
        if (is_file($file_name)) {
            echo "File already exists  [$file_name]: size = " . $this->humanFileSize(filesize($file_name)) . " \n";
            return false;
        }
        $handle = fopen($file_name, 'w');
        $rows = 0;

        try {
            //$start_timestamp = random_int(0, 2145916799);
            for ($ules = 0; $ules < $size; $ules++) {
                $j['meteostationID'] = bin2hex(random_bytes(5));
                $j['timestamp'] = random_int(0, 2145916799);
                $j['temperature'] = NULL;
                $j['pressure'] = NULL;
                $j['humidity'] = NULL;
                $j['wind_max_ms'] = random_int(0, 10) + mt_rand() / mt_getrandmax();
                $j['wind_min_ms'] = random_int(0, 10) + mt_rand() / mt_getrandmax();
                $j['wind_avg'] = random_int(0, 10) + mt_rand() / mt_getrandmax();
                $j['wind_direction'] = random_int(0, 359) + mt_rand() / mt_getrandmax();

                fputcsv($handle, $j);
                $rows++;
            }
        }
        catch (Exception $e) {
                echo 'Failed random: '. $e->getMessage(). PHP_EOL;
            }

        fclose($handle);
        echo "Created file  [$file_name]: $rows rows... size = " . $this->humanFileSize(filesize($file_name)) . " \n";
    }


    /**
     * @param $size
     * @param string $unit
     * @return string
     */
    public function humanFileSize($size, $unit = '')
    {
        if ((!$unit && $size >= 1 << 30) || $unit == 'GB') {
            return number_format($size / (1 << 30), 2) . ' GB';
        }
        if ((!$unit && $size >= 1 << 20) || $unit == 'MB') {
            return number_format($size / (1 << 20), 2) . ' MB';
        }
        if ((!$unit && $size >= 1 << 10) || $unit == 'KB') {
            return number_format($size / (1 << 10), 2) . ' KB';
        }
        return number_format($size) . ' bytes';
    }
}
if (!$argv[1]) {
    echo "Please input dir name with files to processed...";
}
else {
    $dir_to_process = $argv[1];
    $test = new Tests();
    $test->generate_files_list($dir_to_process);
}

}
  
    




