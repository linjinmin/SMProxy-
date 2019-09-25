<?php

require './vendor/autoload.php';

use SMProxy\Handler\Frontend\FrontendAuthenticator;

$serv = new \swoole_server(
    '0.0.0.0',
    3366,
    2,
    1
);

$serv->set(array(
    'worker_num' => 1,
    'daemonize' => false,
    'backlog' => 128,
));

$serv->on('connect', 'my_onConnect');
$serv->on('receive', 'my_onReceive');
$serv->on('Close', 'my_onClose');
$serv->on('WorkerStart', 'my_work');

$serv->start();


function my_work()
{
    $mysql = new \Swoole\Coroutine\MySQL();
    $mysql->connect([
        'host'     => '0.0.0.0',
        'user'     => 'root',
        'port'     => '3366',
        'password' => 'root',
        'database' => 'book',
    ]);
    if ($mysql->connect_errno) {
//        echo 'error'.$mysql ->connect_error;
    }

    $mysql->close();
}

function my_onConnect(\swoole_server $server, int $fd)
{
    /**
     * 连接.
     *
     * @param $server
     * @param $fd
     */

    // 生成认证数据
    $Authenticator = new FrontendAuthenticator();
//
    var_dump('connect'.$fd);
    if ($server->exist($fd)) {
//        $server->send($fd, 123);
    }
    var_dump('connect');
//
//    $server->send($fd, 'test');

}

function my_onReceive(\swoole_server $server, int $fd, int $reactor_id, string $data)
{
    var_dump('receive'.$fd);
//    var_dump($reactor_id);
//    var_dump($data);
}

function my_onClose()
{
    var_dump('close');
}

