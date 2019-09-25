<?php

namespace SMProxy;

use SMProxy\Handler\Frontend\FrontendAuthenticator;
use SMProxy\Handler\Frontend\FrontendConnection;
use function SMProxy\Helper\array_copy;
use function SMProxy\Helper\getBytes;
use function SMProxy\Helper\getMysqlPackSize;
use function SMProxy\Helper\getPackageLength;
use function SMProxy\Helper\getString;
use function SMProxy\Helper\initConfig;
use function SMProxy\Helper\packageSplit;
use SMProxy\Helper\ProcessHelper;
use SMProxy\Log\Log;
use SMProxy\MysqlPacket\AuthPacket;
use SMProxy\MysqlPacket\BinaryPacket;
use SMProxy\MysqlPacket\MySqlPacketDecoder;
use SMProxy\MysqlPacket\MySQLPacket;
use SMProxy\MysqlPacket\OkPacket;
use SMProxy\MysqlPacket\Util\ErrorCode;
use SMProxy\MysqlPacket\Util\RandomUtil;
use SMProxy\MysqlPool\MySQLException;
use SMProxy\MysqlPool\MySQLPool;
use SMProxy\Parser\ServerParse;
use SMProxy\Route\RouteService;
use Swoole\Coroutine;

/**
 * Author: Louis Livi <574747417@qq.com>
 * Date: 2018/10/26
 * Time: 下午6:32.
 */
class SMProxyServer extends BaseServer
{
    // 记录来源  fd => FrontendAuthenticator(认证对象)
    public $source;
    // 当前用户从连接池中获取的client保存， 连接结束后销毁
    public $mysqlClient;
    private $mysqlServer;
    protected $dbConfig;
    //半包体
    public $halfPack;
    // 预处理语句  spl_object_hash(mysqlProxy) => num
    public $stmtId = [];
    public $stmtPrepare = [];

    /**
     * SMProxyServer constructor.
     */
    public function __construct()
    {
        $this->mysqlServer = new \Swoole\Table(array_sum(array_column(CONFIG['database']
            ['databases'], 'maxConns')) * 100, 1);
        // fd
        $this->mysqlServer->column('threadId', \Swoole\Table::TYPE_INT, 64);
        $this->mysqlServer->column('serverVersion', \Swoole\Table::TYPE_STRING, 20);
        $this->mysqlServer->column('pluginName', \Swoole\Table::TYPE_STRING, 64);
        $this->mysqlServer->column('serverStatus', \Swoole\Table::TYPE_INT, 11);
        $this->mysqlServer->create();
        parent::__construct();
    }

    /**
     * 连接.
     *
     * @param $server
     * @param $fd
     */
    public function onConnect(\swoole_server $server, int $fd)
    {
        // 生成认证数据
        $Authenticator = new FrontendAuthenticator();
        $this->source[$fd] = $Authenticator;
        if ($server->exist($fd)) {
            $str = $Authenticator->getHandshakePacket($fd);
            $server->send($fd, $str);
        }
    }

    /**
     * 接收消息.
     *
     * @param $server
     * @param $fd
     * @param $reactor_id
     * @param $data
     */
    public function onReceive(\swoole_server $server, int $fd, int $reactor_id, string $data)
    {
        self::go(function () use ($server, $fd, $reactor_id, $data) {
            if (!isset($this->source[$fd]->auth)) {
                throw new SMProxyException('Must be connected before sending data!');
            }

            if (!isset($this->halfPack[$fd])) {
                // 半包体
                $this->halfPack[$fd] = '';
            }

            if ($this->source[$fd]->auth) {
                // 已完成认证交互， 进入语句执行阶段，
                $headerLength = 4;
            } else {
                // 进行认证交互
                $headerLength = 3;
            }
            $packages = packageSplit($data, $this->source[$fd]->auth ?: false, $headerLength, false, $this->halfPack[$fd]);
            if (empty($packages)) {
                switch ($data) {
                    //获取服务状态信息
                    case "status":
                        $statusData = [];
                        foreach ($this->mysqlServer as $key => $row) {
                            $statusData[$key] = $row;
                        }
                        $server->send($fd, base64_encode(json_encode($statusData)));
                        unset($statusData);
                        break;
                }
            }
            // 对包体循环处理
            foreach ($packages as $package) {
                // 获取包体内容
                $data = $package;
                self::go(function () use ($server, $fd, $reactor_id, $data) {
                    $bin = (new MySqlPacketDecoder())->decode($data);
                    // 第一次进来会先进行自定义的账号密码验证
                    if (!$this->source[$fd]->auth) {
                        // 连接swoole/server 通过配置中的user,password 登录认证
                        $this->auth($bin, $server, $fd);
                    } else {
                        // 解析sql语句， 设置事物，读，锁等等
                        $this->query($bin, $data, $fd);
                        // 判断是读操作还是写操作
                        if (isset($this->connectReadState[$fd]) && true === $this->connectReadState[$fd]) {
                            $model = 'read';
                        } else {
                            $model = 'write';
                        }
                        // 获取读库或者写库的key值
                        $key = $this ->compareModel($model, $server, $fd);
                        if ($data) {
                            if (isset($this->mysqlClient[$fd][$key])) {
                                // 已存在用户连接池直接获取
                                $this->mysqlClient[$fd][$key]->send($data);
                            } else {
                                // 从连接池中获取, mysqlProxy对象
                                $client = MySQLPool::fetch($key, $server, $fd);
                                $result = $client->send($data);
                                if ($result) {
                                    $this->mysqlClient[$fd][$key] = $client;
                                }
                            }
                        }
                        //预处理语句id记录
                        if (isset($this->mysqlClient[$fd][$key])) {
                            $clientId = spl_object_hash($this->mysqlClient[$fd][$key]);
                            switch ($bin ->data[4]) {
                                case MysqlPacket::$COM_STMT_PREPARE:
                                    if (isset($this->stmtId[$clientId])) {
                                        $this->stmtId[$clientId]++;
                                    } else {
                                        $this->stmtId[$clientId] = 1;
                                    }
                                    $this->stmtPrepare[$clientId][$this->stmtId[$clientId]] = $this->stmtId[$clientId];
                                    break;
                                case MySQLPacket::$COM_STMT_CLOSE:
                                    $closeStmtId = getPackageLength($data, 5, 4) - 4;
                                    unset($this->stmtPrepare[$clientId][$closeStmtId]);
                                    break;
                            }
                            unset($clientId);
                        }
                    }
                });
            }
        });
    }

    /**
     * 客户端断开连接.
     *
     * @param \swoole_server $server
     * @param int            $fd
     *
     */
    public function onClose(\swoole_server $server, int $fd)
    {
        if (isset($this->source[$fd])) {
            unset($this->source[$fd]);
        }
        if (isset($this->halfPack[$fd])) {
            unset($this->halfPack[$fd]);
        }
        $connectHasTransaction = false;
        $connectHasAutoCommit  = false;
        if (isset($this->connectHasTransaction[$fd]) && true === $this->connectHasTransaction[$fd]) {
            //回滚未关闭事务
            $connectHasTransaction = true;
            unset($this->connectHasTransaction[$fd]);
        }
        if (isset($this->connectHasAutoCommit[$fd]) && true === $this->connectHasAutoCommit[$fd]) {
            //开启autocommit=0未关闭
            $connectHasAutoCommit = true;
            unset($this->connectHasAutoCommit[$fd]);
        }
        if (isset($this->mysqlClient[$fd])) {
            foreach ($this->mysqlClient[$fd] as $key => $mysqlClient) {
                $model = explode(DB_DELIMITER, $key)[0];
                if ($model == 'write') {
                    if (isset($mysqlClient ->client) && $mysqlClient ->client) {
                        if ($connectHasTransaction) {
                            // 发送关闭事物
                            $mysqlClient->send(getString([9, 0, 0, 0, 3, 82, 79, 76, 76, 66, 65, 67, 75]));
                        }
                        if ($connectHasAutoCommit) {
                            // 发送autocommit
                            $mysqlClient->send(getString([
                                17, 0, 0, 0, 3, 115, 101, 116, 32, 97, 117, 116, 111, 99, 111, 109, 109, 105, 116, 61, 49,
                            ]));
                        }
                    }
                }
                //处理预处理语句连接断开未关闭
                $clientId = spl_object_hash($mysqlClient);
                if (isset($this->stmtPrepare[$clientId])) {
                    $stmtIdes = $this->stmtPrepare[$clientId] ?? [];
                    if (!empty($stmtIdes)) {
                        foreach ($stmtIdes as $stmtId) {
                            $mysqlClient->send(getString(array_merge([5, 0, 0, 0, 25], getMysqlPackSize($stmtId, 4))));
                        }
                    }
                    unset($this->stmtPrepare[$clientId]);
                }
                unset($clientId);
                MySQLPool::recycle($mysqlClient);
            }
            unset($this->mysqlClient[$fd]);
        }
        if (isset($this->connectReadState[$fd])) {
            unset($this->connectReadState[$fd]);
        }
        parent::onClose($server, $fd);
    }

    /**
     * WorkerStart.
     *
     * @param \swoole_server $server
     * @param int $worker_id
     */
    public function onWorkerStart(\swoole_server $server, int $worker_id)
    {
//        self::go(function () use ($server, $worker_id) {
//            if ($worker_id >= CONFIG['server']['swoole']['worker_num']) {
//                ProcessHelper::setProcessTitle('SMProxy task    process');
//            } else {
//                ProcessHelper::setProcessTitle('SMProxy worker  process');
//            }
//            try {
                $this->dbConfig = $this->parseDbConfig(initConfig(CONFIG_PATH));
//                //初始化链接
                MySQLPool::init($this->dbConfig, $this->mysqlServer);
//            } catch (MySQLException $exception) {
//                self::writeErrorMessage($exception, 'mysql');
//                $server ->shutdown();
//                return;
//            } catch (SMProxyException $exception) {
//                self::writeErrorMessage($exception, 'system');
//                $server ->shutdown();
//                return;
//            }
//            if ($worker_id === (CONFIG['server']['swoole']['worker_num'] - 1)) {
                try {
                    Coroutine::sleep(0.1);
                    $this ->setStartConns();
                } catch (MySQLException $exception) {
                    self::writeErrorMessage($exception, 'mysql');
                    $server ->shutdown();
                    return;
                }
                $system_log = Log::getLogger('system');
                $system_log->info('Worker started!');
                echo 'Worker started!', PHP_EOL;
//            }
//        });
    }

    /**
     * 设置服务启动连接数
     *
     * @throws MySQLException
     */
    private function setStartConns()
    {
//        $clients = [];
//        $this->dbConfig = ['writeSΜbook' => array_shift($this->dbConfig)];
        $mysql = new \Swoole\Coroutine\MySQL();
        $mysql->connect([
            'host'     => '0.0.0.0',
            'user'     => 'root',
            'port'     => 3366,
            'password' => 'root',
            'database' => 'book',
        ]);
        if ($mysql ->connect_errno) {
                    var_dump($mysql ->connect_error);
        }

        $res = $mysql->query('/*SMProxy read*/select sleep(0.1)');

        $res = $mysql->close();

//        foreach ($this->dbConfig as $key => $value) {
//            if (count(explode(DB_DELIMITER, $key)) < 2) {
//                continue;
//            }
//            //测试数据库host port是否可连接
////            $test_client = new \Swoole\Coroutine\Client(SWOOLE_SOCK_TCP);
////            if (!$test_client->connect($value['serverInfo']['host'], $value['serverInfo']['port'], $value['serverInfo']['timeout'])) {
////                throw new MySQLException('connect ' . explode(DB_DELIMITER, $key)[0] .
////                    ' ' . explode(DB_DELIMITER, $key)[1] . ' failed, ErrorCode: ' . $test_client->errCode . "\n");
////            }
////            $test_client->close();
//            //初始化连接
//            if (!isset($value['startConns'])) {
//                $value['startConns'] = 1;
//            }
//
//            var_dump('connect');
//            $mysql = new \Swoole\Coroutine\MySQL();
//            $mysql->connect([
//                'host'     => CONFIG['server']['host'],
//                'user'     => CONFIG['server']['user'],
//                'port'     => CONFIG['server']['port'],
//                'password' => CONFIG['server']['password'],
//                'database' => explode(DB_DELIMITER, $key)[1],
//            ]);
//
////            while ($value['startConns']) {
////                //初始化startConns
////                $mysql = new \Swoole\Coroutine\MySQL();
////                $mysql->connect([
////                    'host'     => CONFIG['server']['host'],
////                    'user'     => CONFIG['server']['user'],
////                    'port'     => CONFIG['server']['port'],
////                    'password' => CONFIG['server']['password'],
////                    'database' => explode(DB_DELIMITER, $key)[1],
////                ]);
//                if ($mysql ->connect_errno) {
//                    throw new MySQLException(CONFIG['server']['host'] . ':' . CONFIG['server']['port'] . $mysql ->connect_error);
//                }
////                $mysql->setDefer();
////                switch (explode(DB_DELIMITER, $key)[0]) {
////                    case 'read':
////                        $mysql->query('/*SMProxy test sql*/select sleep(0.1)');
////                        break;
////                    case 'write':
////                        $mysql->query('/*SMProxy test sql*/set autocommit=1');
////                        break;
////                }
////                $clients[] = $mysql;
////                $value['startConns']--;
////                var_dump('donw -------');
////                exit();
////            }
//        }
//        foreach ($clients as $client) {
//            $client->recv();
//            if ($client ->errno) {
//                throw new MySQLException($client ->error);
//            }
//            $client->close();
//        }
//        unset($clients);
    }

    /**
     * 验证账号
     *
     * @param \swoole_server $server
     * @param int $fd
     * @param string $user
     * @param string $password
     *
     * @return bool
     */
    private function checkAccount(\swoole_server $server, int $fd, string $user, array $password)
    {
        $checkPassword = $this->source[$fd]
            ->checkPassword($password, CONFIG['server']['password']);
        return CONFIG['server']['user'] == $user && $checkPassword;
    }

    /**
     * 验证账号失败
     *
     * @param \swoole_server $server
     * @param int $fd
     * @param int $serverId
     *
     * @throws MySQLException
     */
    private function accessDenied(\swoole_server $server, int $fd, int $serverId)
    {
        $message = 'SMProxy@access denied for user \'' . $this->source[$fd]->user . '\'@\'' .
            $server ->getClientInfo($fd)['remote_ip'] . '\' (using password: YES)';
        $errMessage = self::writeErrMessage($serverId, $message, ErrorCode::ER_ACCESS_DENIED_ERROR, 28000);
        if ($server->exist($fd)) {
            $server->send($fd, getString($errMessage));
        }
        throw new MySQLException($message);
    }

    /**
     * 判断model
     *
     * @param string $model
     * @param \swoole_server $server
     * @param int $fd
     *
     * @return string
     * @throws MySQLException
     */
    private function compareModel(string $model, \swoole_server $server, int $fd)
    {
        /**
         * 拼接数据库键名
         *
         * @param int $fd
         * @param string $model
         *
         * @return string
         */
        $spliceKey = function (int $fd, string $model) {
            return $this->source[$fd]->database ? $model . DB_DELIMITER . $this->source[$fd]->database : $model;
        };
        switch ($model) {
            case 'read':
                $key = $spliceKey($fd, $model);
                //如果没有读库 默认用写库
                if (!isset($this->dbConfig[$key])) {
                    $model = 'write';
                    $key = $spliceKey($fd, $model);
                    //如果没有写库
                    $this->existsDBKey($server, $fd, $model, $key);
                }
                break;
            case 'write':
                $key = $spliceKey($fd, $model);
                //如果没有写库
                $this->existsDBKey($server, $fd, $model, $key);
                break;
            default:
                $key = 'write' . DB_DELIMITER . $this->source[$fd]->database;
                break;
        }
        return $key;
    }


    /**
     * 判断配置文件键是否存在
     *
     * @param \swoole_server $server
     * @param int $fd
     * @param string $model
     * @param string $key
     *
     * @throws MySQLException
     */
    private function existsDBKey(\swoole_server $server, int $fd, string $model, string $key)
    {
        //如果没有写库则抛出异常
        if (!isset($this->dbConfig[$key])) {
            $message = 'SMProxy@Database config ' . ($this->source[$fd]->database ?: '') . ' ' . $model .
                ' is not exists!';
            $errMessage = self::writeErrMessage(1, $message, ErrorCode::ER_SYNTAX_ERROR, 42000);
            if ($server->exist($fd)) {
                $server->send($fd, getString($errMessage));
            }
            throw new MySQLException($message);
        }
    }

    /**
     * 验证
     *
     * @param BinaryPacket $bin
     * @param \swoole_server $server
     * @param int $fd
     *
     * @throws MySQLException
     */
    private function auth(BinaryPacket $bin, \swoole_server $server, int $fd)
    {
        // 如果数据长度是20, -- 可能自定义的,  4-20是密码, 最后4位不知道干啥
        if ($bin->data[0] == 20) {
            // 密码长度是16 , 判断账号密码
            $checkAccount = $this->checkAccount($server, $fd, $this->source[$fd]->user, array_copy($bin->data, 4, 20));
            if (!$checkAccount) {
                // 发送ERROR报文
                $this ->accessDenied($server, $fd, 4);
            } else {
                if ($server->exist($fd)) {
                    // 发送OK报文
                    $server->send($fd, getString(OkPacket::$SWITCH_AUTH_OK));
                }
                // 认证标志设置为true
                $this->source[$fd]->auth = true;
            }
        } elseif ($bin->data[4] == 14) {
            // 序号等于14
            if ($server->exist($fd)) {
                // 无需认证即登录
                $server->send($fd, getString(OkPacket::$OK));
            }
        } else {
            $authPacket = new AuthPacket();
            // 读取报文信息 登录认证报文
            $authPacket->read($bin);
            // 判断账号密码
            $checkAccount = $this->checkAccount($server, $fd, $authPacket->user ?? '', $authPacket->password ?? []);
            if (!$checkAccount) {
                // 密码校验失败
                if ($authPacket->pluginName == 'mysql_native_password') {
                    // 发送ERROR报文
                    $this ->accessDenied($server, $fd, 2);
                } else {
                    // 记录用户数据
                    $this->source[$fd]->user = $authPacket ->user;
                    $this->source[$fd]->database = $authPacket->database;
                    // 填充数
                    $this->source[$fd]->seed = RandomUtil::randomBytes(20);
                    // 发送EOF报文
                    $authSwitchRequest = array_merge(
                        [254],
                        getBytes('mysql_native_password'),
                        [0],
                        $this->source[$fd]->seed,
                        [0]
                    );
                    if ($server->exist($fd)) {
                        $server->send($fd, getString(array_merge(getMysqlPackSize(count($authSwitchRequest)), [2], $authSwitchRequest)));
                    }
                }
            } else {
                // 账号正确 发送OK报文， 并记录数据
                if ($server->exist($fd)) {
                    $server->send($fd, getString(OkPacket::$AUTH_OK));
                }
                $this->source[$fd]->auth = true;
                $this->source[$fd]->database = $authPacket->database;
            }
        }
    }

    /**
     * 语句解析处理
     *
     * @param BinaryPacket $bin
     * @param string $data
     * @param int $fd
     *
     * @throws MySQLException
     */
    private function query(BinaryPacket $bin, string &$data, int $fd)
    {
        // &/*SMProxy test sql*/select sleep(0.1)
        // 去除空格
        $trim_data = rtrim($data);
        // 获取长度
        $data_len  = strlen($trim_data);
        // 解析客户端请求报文 , 第一个字节是命令
        switch ($bin->data[4]) {
            case MySQLPacket::$COM_INIT_DB:
                // 切换数据库
                // just init the frontend
                break;
            case MySQLPacket::$COM_STMT_PREPARE: //预处理SQL语句
            case MySQLPacket::$COM_QUERY: //SQL查询请求
                $connection = new FrontendConnection();
                // 判断语句命令类型
                $queryType = $connection->query($bin); // querytype = 7
                // 参数截取消息体全部内容, 判断是否有强制要求指定读还是写数据库
                $hintArr   = RouteService::route(substr($data, 5, strlen($data) - 5));
                if (isset($hintArr['db_type'])) {
                    switch ($hintArr['db_type']) {
                        case 'read':
                            if ($queryType == ServerParse::DELETE || $queryType == ServerParse::INSERT ||
                                $queryType == ServerParse::REPLACE || $queryType == ServerParse::UPDATE ||
                                $queryType == ServerParse::DDL) {
                                $this->connectReadState[$fd] = false;
                                $system_log = Log::getLogger('system');
                                $system_log->warning("should not use hint 'db_type' to route 'delete', 'insert', 'replace', 'update', 'ddl' to a slave db.");
                            } else {
                                // 设置为读语句
                                $this->connectReadState[$fd] = true;
                            }
                            break;
                        case 'write':
                            // 设置为写
                            $this->connectReadState[$fd] = false;
                            break;
                        default:
                            $this->connectReadState[$fd] = false;
                            $system_log = Log::getLogger('system');
                            $system_log->warning("use hint 'db_type' value is not found.");
                            break;
                    }
                } elseif (ServerParse::SELECT == $queryType ||
                    ServerParse::SHOW == $queryType ||
                    (ServerParse::SET == $queryType && false === strpos($data, 'autocommit', 4)) ||
                    ServerParse::USE == $queryType
                ) {
                    //处理读操作, 不存在事物
                    if (!isset($this->connectHasTransaction[$fd]) || !$this->connectHasTransaction[$fd]) {
                        if ($data_len > 6 && (('u' == $trim_data[$data_len - 6] || 'U' == $trim_data[$data_len - 6]) &&
                            ServerParse::UPDATE == ServerParse::uCheck($trim_data, $data_len - 6, false))) {
                            //判断悲观锁 , 不理解为啥读操作需要设置悲观锁, 而且判断条件无法理解
                            $this->connectReadState[$fd] = false;
                        } else {
                            $this->connectReadState[$fd] = true;
                        }
                    }
                } elseif (ServerParse::START == $queryType || ServerParse::BEGIN == $queryType
                ) {
                    //处理事务
                    $this->connectHasTransaction[$fd] = true;
                    $this->connectReadState[$fd] = false;
                } elseif (ServerParse::SET == $queryType && false !== strpos($data, 'autocommit', 4) &&
                    0 == $trim_data[$data_len - 1]) {
                    //处理autocommit事务
                    $this->connectHasAutoCommit[$fd] = true;
                    $this->connectHasTransaction[$fd] = true;
                    $this->connectReadState[$fd] = false;
                } elseif (ServerParse::SET == $queryType && false !== strpos($data, 'autocommit', 4) &&
                    1 == $trim_data[$data_len - 1]) {
                    // set 语句
                    $this->connectHasAutoCommit[$fd] = false;
                    $this->connectReadState[$fd] = false;
                } elseif (ServerParse::COMMIT == $queryType || ServerParse::ROLLBACK == $queryType) {
                    //事务提交
                    $this->connectHasTransaction[$fd] = false;
                } else {
                    $this->connectReadState[$fd] = false;
                }
                break;
            case MySQLPacket::$COM_PING:
                break;
            case MySQLPacket::$COM_QUIT:
                //禁用客户端退出
                $data = '';
                break;
            case MySQLPacket::$COM_PROCESS_KILL:
                break;
            case MySQLPacket::$COM_STMT_EXECUTE:
                break;
            case MySQLPacket::$COM_STMT_CLOSE:
                break;
            case MySQLPacket::$COM_HEARTBEAT:
                break;
            default:
                break;
        }
    }
}
