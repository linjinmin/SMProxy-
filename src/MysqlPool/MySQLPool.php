<?php

namespace SMProxy\MysqlPool;

use SMProxy\Base;
use function SMProxy\Helper\getString;
use SMProxy\MysqlPacket\Util\ErrorCode;
use SMProxy\MysqlProxy;
use Swoole\Coroutine\Client;

/**
 * Author: Louis Livi <574747417@qq.com>
 * Date: 2018/11/6
 * Time: 上午10:52.
 */
class MySQLPool extends Base
{
    protected static $init = false;
    // 空闲连接池   key => [mysqlProxy]
    protected static $spareConns = [];
    // 正在使用中的连接池 key => spl_object_hash(mysqlProxy) => mysqlProxy
    protected static $busyConns  = [];
    // 数据库连接配置 -> 处理后的
    protected static $connsConfig;
    // 对应 mysqlProxy->connName(key值)
    protected static $connsNameMap = [];
    // 根据意思理解 待取数量      key => num
    protected static $pendingFetchCount = [];
    //   key => num
    protected static $resumeFetchCount  = [];
    //   key => spl_object_hash(mysqlProxy)
    protected static $yieldChannel  = [];
    // 初始化连接池   key => num  对应数据库配置的数量
    protected static $initConnCount = [];
    protected static $lastConnsTime = [];
    protected static $mysqlServer;

    /**
     * @param array $connsConfig
     *
     * @throws MySQLException
     */
    public static function init(array $connsConfig, &$mysqlServer)
    {
        if (self::$init) {
            return;
        }
        self::$connsConfig = $connsConfig;
        foreach ($connsConfig as $name => $config) {
            self::$spareConns[$name] = [];
            self::$busyConns[$name] = [];
            self::$pendingFetchCount[$name] = 0;
            self::$resumeFetchCount[$name]  = 0;
            self::$initConnCount[$name] = 0;
            if ($config['maxSpareConns'] <= 0 || $config['maxConns'] <= 0) {
                throw new MySQLException("Invalid maxSpareConns or maxConns in {$name}");
            }
        }
        self::$mysqlServer = $mysqlServer;
        self::$init = true;
    }

    /**
     * 回收连接。
     *
     * @param MysqlProxy $conn
     * @param bool       $busy
     *
     */
    public static function recycle(MysqlProxy $conn, bool $busy = true)
    {

        //回收Busy池
        self::go(function () use ($conn, $busy) {
            if (!self::$init) {
                throw new MySQLException('Should call MySQLPool::init.');
            }
            // 获取conn hash对象
            $id = spl_object_hash($conn);
            // 数据库的key 值
            $connName = self::$connsNameMap[$id];
            if ($busy) {
                // 关闭busy 对应key的连接
                if (isset(self::$busyConns[$connName][$id])) {
                    unset(self::$busyConns[$connName][$id]);
                } else {
                    throw new MySQLException('Unknow MySQL connection.');
                }
            }
            // 处理空闲连接池
            $connsPool = &self::$spareConns[$connName];
            if (((count($connsPool) + self::$initConnCount[$connName]) >= self::$connsConfig[$connName]['maxSpareConns']) &&
                ((microtime(true) - self::$lastConnsTime[$id]) >= ((self::$connsConfig[$connName]['maxSpareExp']) ?? 0))
            ) {
                // 超过最大空闲连接数  , 并且时间超过回收阶段
                $threadName = $connName . DB_DELIMITER . $conn->mysqlServer->threadId;
                if (self::$mysqlServer->exist($threadName)) {
                    self::$mysqlServer->del($threadName);
                }
                if ($conn->client->isConnected()) {
                    $conn->client->close();
                }
                unset($threadName);
                unset(self::$connsNameMap[$id]);
            } else {
                // 如果连接丢失，重新连接
                if (!$conn->client->isConnected()) {
                    unset(self::$connsNameMap[$id]);
                    $conn = self::initConn($conn->server, $conn->serverFd, $connName);
                    $id = spl_object_hash($conn);
                }
                // 添加到Spare 连接池
                $connsPool[] = $conn;
                if (self::$pendingFetchCount[$connName] > 0) {
                    ++self::$resumeFetchCount[$connName];
                    self::$yieldChannel[$connName]->push($id);
                }
            }
        });
    }

    /**
     * 获取连接.
     *
     * @param $connName
     * @param \swoole_server $server
     * @param $fd
     *
     * @return bool|mixed|MysqlProxy
     *
     * @throws MySQLException
     * @throws \SMProxy\SMProxyException
     */
    public static function fetch(string $connName, \swoole_server $server, int $fd)
    {
        if (!self::$init) {
            throw new MySQLException('Should call MySQLPool::init!');
        }
        if (!isset(self::$connsConfig[$connName])) {
            throw new MySQLException("Unvalid connName: {$connName}.");
        }
        // 获取对应key(即指定数据库)空闲连接池数据
        $connsPool = &self::$spareConns[$connName];
        if (!empty($connsPool) && count($connsPool) > self::$resumeFetchCount[$connName]) {
            // 从连接池中取出数据
            $conn = array_pop($connsPool);
            if (!$conn->client->isConnected()) {
                // 连接断开 重新连接
                return self::reconnect($server, $fd, $conn, $connName);
            } else {
                // 将conn->serverfd 指定用户
                $conn->serverFd = $fd;
                $id = spl_object_hash($conn);
                //将该连接丢入到busy连接池
                self::$busyConns[$connName][$id] = $conn;
                // 对应Conn的使用时间
                self::$lastConnsTime[$id] = microtime(true);
                // 返回连接
                return $conn;
            }
        }

        // 走到这里 connspool应该是0
        if ((count(self::$busyConns[$connName]) + count($connsPool) + self::$pendingFetchCount[$connName] +
                self::$initConnCount[$connName]) >= self::$connsConfig[$connName]['maxConns']) {
            if (!isset(self::$yieldChannel[$connName])) {
                self::$yieldChannel[$connName] = new \Swoole\Coroutine\Channel(1);
            }
            // 待取+1
            ++self::$pendingFetchCount[$connName];
            // 等待chan返回连接池对象
            $client = self::coPop(self::$yieldChannel[$connName], self::$connsConfig[$connName]['serverInfo']['timeout']);
            // 为空， 繁忙 无连接池
            if (false === $client) {
                --self::$pendingFetchCount[$connName];
                $message = 'SMProxy@Reach max connections! Cann\'t pending fetch!';
                $errMessage = self::writeErrMessage(1, $message, ErrorCode::ER_HAS_GONE_AWAY);
                if ($server->exist($fd)) {
                    // 发送错误数据
                    $server->send($fd, getString($errMessage));
                }
                throw new MySQLException($message);
            }
            // 等到到连接池
            --self::$resumeFetchCount[$connName];
            if (!empty($connsPool)) {
                $conn = array_pop($connsPool);
                if (!$conn->client->isConnected()) {
                    $conn = self::reconnect($server, $fd, $conn, $connName);
                    --self::$pendingFetchCount[$connName];

                    return $conn;
                } else {
                    $conn->serverFd = $fd;
                    $id = spl_object_hash($conn);
                    self::$busyConns[$connName][$id] = $conn;
                    self::$lastConnsTime[$id] = microtime(true);
                    --self::$pendingFetchCount[$connName];

                    return $conn;
                }
            } else {
                return false; //should not happen
            }
        }

        return self::initConn($server, $fd, $connName);
    }

    /**
     * 初始化链接.
     *
     * @param \swoole_server $server
     * @param int            $fd
     * @param string         $connName
     *
     * @return mixed
     *
     * @throws MySQLException
     * @throws \SMProxy\SMProxyException
     */
    public static function initConn(\swoole_server $server, int $fd, string $connName, $tryStep = 0)
    {
        // 对应key初始化连接池的数量++
        ++self::$initConnCount[$connName];
        $chan = new \Swoole\Coroutine\Channel(1);
        $conn = new MysqlProxy($server, $fd, $chan);
        // 获取对应key值的serverInfo
        $serverInfo = self::$connsConfig[$connName]['serverInfo'];
        // 判断有没有SM标示， 处理后的数据库连接包含SM标识
        if (false == strpos($connName, DB_DELIMITER)) {
            $conn->database = 0;
            $conn->model    = $connName;
        } else {
            // 获取数据库名
            $conn->database = substr($connName, strpos($connName, DB_DELIMITER) + strlen(DB_DELIMITER));
            // read or write
            $conn->model    = substr($connName, 0, strpos($connName, DB_DELIMITER));
        }

        // 获取数据库账号
        $conn->account  = $serverInfo['account'];
        // 符号编码
        $conn->charset  = self::$connsConfig[$connName]['charset'];
        // 通过clinet 去连接mysql
        if (false == $conn->connect($serverInfo['host'], $serverInfo['port'], $serverInfo['timeout'] ?? 0.1)) {
            // 连接失败 对应的初始化key => num -1
            --self::$initConnCount[$connName];
            $message = 'SMProxy@MySQL server has gone away';
            $errMessage = self::writeErrMessage(1, $message, ErrorCode::ER_HAS_GONE_AWAY);
            if ($server->exist($fd)) {
                // 发送ERROR报文数据到客户端
                $server->send($fd, getString($errMessage));
            }
            throw new MySQLException($message);
        }

        //获取mysql clinet
        $client = self::coPop($chan, $serverInfo['timeout'] * 3);
        if ($client === false) {
            // 初始池-1
            --self::$initConnCount[$connName];
            if ($tryStep < 3) {
                // 重新连接
                return self::initConn($server, $fd, $connName, ++$tryStep);
            } else {
                $message = 'SMProxy@Connection ' . $serverInfo['host'] . ':' . $serverInfo['port'] .
                    ' waiting timeout, timeout=' . $serverInfo['timeout'];
                $errMessage = self::writeErrMessage(1, $message, ErrorCode::ER_HAS_GONE_AWAY);
                if ($server->exist($fd)) {
                    // 发送连接error
                    $server->send($fd, getString($errMessage));
                }
                throw new MySQLException($message);
            }
        }
        
        $id = spl_object_hash($client);
        // 映射connnamemap  hash(mysqlProxy) => connname
        self::$connsNameMap[$id] = $connName;
        // 加入到繁忙池
        self::$busyConns[$connName][$id] = $client;
        // 重制时间
        self::$lastConnsTime[$id] = microtime(true);
        // 初始池这样看 记录是0
        --self::$initConnCount[$connName];
        //保存服务信息
        $threadName = $connName . DB_DELIMITER . $conn->mysqlServer->threadId;
        // 记录到mysqlserver对象
        self::$mysqlServer->set($threadName, [
            "threadId"      => $client->mysqlServer->threadId,
            "serverVersion" => $client->mysqlServer->serverVersion,
            "pluginName"    => $client->mysqlServer->pluginName,
            "serverStatus"  => $client->mysqlServer->serverStatus,
        ]);
        unset($threadName);
        return $client;
    }

    /**
     * 销毁连接。
     *
     * @param Client $cli
     * @param string $connName
     *
     */
    public static function destruct(Client $cli, string $connName)
    {
        self::go(function () use ($cli, $connName) {
            // 关闭连接
            if ($cli->isConnected()) {
                $cli ->close();
            }
            $proxyConn = false;
            foreach (self::$spareConns[$connName] as $key => $conn) {
                // 销毁空闲连接池对应的 连接对象
                if (spl_object_hash($conn ->client) == spl_object_hash($cli)) {
                    $proxyConn = $conn;
                    unset(self::$spareConns[$connName][$key]);
                    break;
                }
            }
            if ($proxyConn) {
                self::recycle($proxyConn, false);
            }
        });
    }

    /**
     * 断重链.
     *
     * @param \swoole_server      $server
     * @param int                 $fd
     * @param \SMProxy\MysqlProxy $conn
     * @param string              $connName
     *
     * @return mixed
     *
     * @throws \SMProxy\MysqlPool\MySQLException
     * @throws \SMProxy\SMProxyException
     */
    public static function reconnect(\swoole_server $server, int $fd, MysqlProxy $conn, string $connName)
    {
        if ($conn->client->isConnected()) {
            $conn->client->close();
        }
        $old_id = spl_object_hash($conn);
        unset(self::$busyConns[$connName][$old_id]);
        unset(self::$connsNameMap[$old_id]);
        self::$lastConnsTime[$old_id] = 0;

        return self::initConn($server, $fd, $connName);
    }
}
