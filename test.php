<?php


//
//$conn = new \mysqli('127.0.0.1', 'root', 'root', 'book', '3306');
//
//if ($conn->connect_error) {
//    exit('error');
//}
//
//$sql = 'select * from user limit 1';
//
//$result = $conn->query($sql);
//
//if($result->num_rows > 0) {
//
//    while ($row = $result->fetch_assoc()) {
//        var_dump($row);
//    }
//
//} else {
//    echo 'none';
//}
//
//$conn->close();


//header("content-type:text/html;charset=cp936");
//$url = 'https://m.500.com/info/kaijiang/hnd4j1/19094.shtml#';
//$result = file_get_contents($url);
////var_dump($result);
////$encode = mb_detect_encoding($result, array("ASCII", 'UTF-8', "GB2312", "GBK", 'BIG5'));
//echo mb_convert_encoding($result, 'UTF-8', $result);
//exit;


var_dump(strlen('/*SMProxy test sql*/select sleep(0.1))'));



//初始化startConns
//go(function() {
//    $mysql = new \Swoole\Coroutine\MySQL();
//    $mysql->connect([
//        'host'     => '0.0.0.0',
//        'user'     => 'root',
//        'port'     => '9501',
//        'password' => 'root',
//        'database' => 'book',
//    ]);
//    if ($mysql->connect_errno) {
//        echo $mysql ->connect_error;
//    }
//});






