<?php

namespace thinkphp\swoole;
require_once('SwooleMySQL.php');

use thinkphp\swoole\SwooleMySQL as SwooleMySQL;
use think\facade\Session;
use think\facade\Cache;

class SwoolePDO extends \PDO
{
    protected $config = [];
    protected $smPool = [];
    protected $freeConnect = [];
    protected $workConnect = [];
    protected $lastConnectId = 1;
    protected $insert_id = null;
    protected $transactions = [];
    const CONNECT_NUM = 10;

    public function __construct($dsn = null, $username = null, $password = null, $params= null )
    {
        if (!empty($dsn)) {
            $arr = explode(";",$dsn);
            $config = [];
            foreach ($arr as $key => $value) {
                if (strpos($value, ":")) {
                    $hostArr = explode(":", $value);
                    $temp = explode("=", $hostArr[1]);  
                } else {
                    $temp = explode("=", $value);
                } 
                $config[$temp[0]] = $temp[1];
            }
            $this->config = [
                'host'        => $config['host'] ?: "127.0.0.1",
                'port'        => $config['port'] ?: 3306,
                'database'    => $config['dbname'] ?: 'test',
                'user'        => $username,
                'password'    => $password,
                'charset'     => $config['charset'] ?: 'utf-8',
                'timeout'     => !empty($config['timeout']) ? $config['timeout'] : 5,
            ];
            $this->connect($this->config);
        }
    }

    public function connect($config = []) {
        $this->config = $config; 
        for ($i=0; $i++; $i<self::CONNECT_NUM) {
            $this->newConnect($this->config);
        }
        $this->removeLock('alloc_connect');
    }

    /**
     * 新建连接
     * @param array $serverInfo
     * @throws Exception
     */
    protected function newConnect(array $serverInfo)
    {
        $sm = new SwooleMySQL();
        $sm->connect($serverInfo);

        if ($sm->connected === false) {
            $msg = sprintf('Cannot connect to the database: %s',
                $sm->connect_errno ? $sm->connect_error : $sm->error
            );
            $code = $sm->connect_errno ?: $sm->errno;
            throw new \Exception($msg, $code);
        }
        $sm->id = $this->lastConnectId++;
        $this->smPool[$sm->id] = $sm;
        $this->freeConnect[] = $sm->id;
    }

    /**
     * 新分配连接，没有空闲的就新建
     * @return [type] [description]
     */
    protected function allocConnect($trans = false) {
        if (!empty($this->freeConnect)) {
            $connectId =  array_shift($this->freeConnect);
            if (empty($connectId)) {
                throw new \Exception("alloc connect fail", "2192012");
            }
            if (!$trans) {
                $this->workConnect[] = $connectId;
            } else {
                $session_id = Session::getId();
                $this->transactions[$session_id] = $connectId;
            }
            return $this->smPool[$connectId];
        } else {
            $this->newConnect($this->config);
            return $this->allocConnect($trans);
        }

    }


    public function addLock($key)
    {
        $starttime = microtime(true);
        while (1) {
            $num = rand(1, 10);
            if (!Cache::get($key)) {
                usleep($num);
                if (!Cache::get($key)) {
                    Cache::set($key, 1, 30);
                    $time = microtime(true) - $starttime;
                    return true;
                }
            }
            usleep($num*100);
        }
        return false;
    }

    public function removeLock($key)
    {
        Cache::set($key, 0, 30);
        return true;
    }

    /**
     * 释放连接
     * @return [type] [description]
     */
    public function freeConnect($connectId, $trans = false) {
        
        if(!$trans) {
            $this->freeConnect[] = $connectId;
            $this->workConnect = array_diff($this->workConnect, [$connectId]);
            return ;
        }

        $session_id = Session::getId();
        if (isset($this->transactions[$session_id])) {
            $this->freeConnect[] = $this->transactions[$session_id];
            unset($this->transactions[$session_id]);
        }
    }

    /**
     * 获取连接
     * @return [type] [description]
     */
    protected function getConnect($trans = false) {
        $session_id = Session::getId();
        if ($trans && isset($this->transactions[$session_id])) {
            return $this->transactions[$session_id]; 
        }
        if(!$this->addLock('alloc_connect')){
            throw new \Exception('get alloc_connect lock fail', 2312110);
        }
        $sm = $this->allocConnect($trans);
        $this->removeLock('alloc_connect');
        return $sm;
    }

    public function prepare($statement, $options = null)
    {
        if (strpos($statement, ':') !== false) {
            $i = 0;
            $bindKeyMap = [];
            $statement = preg_replace_callback(
                '/:(\w+)\b/',
                function ($matches) use (&$i, &$bindKeyMap) {
                    $bindKeyMap[":".$matches[1]] = $i++;

                    return '?';
                },
                $statement
            );
        }        
        $sm = $this->getConnect();
        $swStatement = $sm->prepare($statement);
        if ($swStatement === false) {
            throw new \Exception($sm->error, $sm->errno);
        }
        $swoolePDOStatement = new SwoolePDOStatement($swStatement, $this, $sm->id);
        if (!empty($bindKeyMap)) {
            $swoolePDOStatement->bindKeyIndex = $bindKeyMap;
        }
        
        return $swoolePDOStatement;
    }

    public function beginTransaction()
    {
        if(!$this->addLock('alloc_connect')){
            throw new \Exception('get  alloc_connect lock fail', 2312110);
        }
        $sm = $this->allocConnect(true);
        $this->removeLock('alloc_connect');
        $sm->begin();
    }

    public function commit()
    {
        $sm = $this->getConnect(true);
        $sm->commit();
        $this->freeConnect($sm->id, true);
    }

    public function rollBack()
    {
        $sm = $this->getConnect(true);
        $sm->rollback();
        $this->freeConnect($sm->id, true);
    }

    public function query($statement, $mode = \PDO::ATTR_DEFAULT_FETCH_MODE, $arg3 = null, array $ctorargs = [])
    {
        $sm = $this->getConnect();
        $result = $sm->query($statement, array_get($ctorargs, 'timeout', 0.0));
        $this->insert_id = $sm->insert_id;
        $this->freeConnect($sm->id);
        if ($result === false) {
            throw new \Exception($sm->error, $sm->errno);
        }
        return $result;
    }

    public function exec($statement)
    {
        return $this->query($statement);
    }

    public function lastInsertId($name = null)
    {
        return $this->insert_id;
    }

    public function rowCount()
    {
        $sm = $this->getConnect();
        return $sm->affected_rows;
    }

    public function quote($string, $parameter_type = \PDO::PARAM_STR)
    {
        //TODO
        return $string;
    }

    public function errorCode()
    {
        $sm = $this->getConnect();
        return $sm->errno;
    }

    public function errorInfo()
    {
        $sm = $this->getConnect();
        return [
            $sm->errno,
            $sm->errno,
            $sm->error,
        ];
    }

    public function inTransaction()
    {
    }

    public function getAttribute($attribute)
    {
    }

    public function setAttribute($attribute, $value)
    {
        // TODO
        return false;
    }

    public static function getAvailableDrivers()
    {
        return ['mysql'];
    }

    public function __destruct()
    {
        foreach ($this->smPool as $sm) {
            $sm->close();
        }
        $this->smPool = [];
        $this->workConnect = [];
        $this->freeConnect = [];
    }
}
