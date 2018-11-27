<?php

namespace thinkphp\swoole;

use Swoole\Coroutine\MySQL\Statement as SwooleStatement;

class SwoolePDOStatement extends \PDOStatement
{
    protected $statement;
    protected $bindParams = [];
    protected $result;
    public $bindKeyIndex = [];
    private $dbPool = null;
    public $connectId = null;

    public function __construct(SwooleStatement $statement, SwoolePDO &$dbPool, $connectId)
    {
        $this->statement = $statement;
        $this->dbPool = &$dbPool;
        $this->connectId = $connectId;
    }

    public function rowCount()
    {
        return $this->statement->affected_rows;
    }

    public function bindValue($parameter, $value, $data_type = \PDO::PARAM_STR)
    {
        if(!isset($this->bindKeyIndex[$parameter])) {
            throw new \Exception("bind param keyName not find:" . $parameter, 901213);
        }
        $index = $this->bindKeyIndex[$parameter];
        $this->bindParams[$index] = $value;
        return true;
    }

    /**
     * @param array $input_parameters
     * @return bool
     * @throws StatementException
     */
    public function execute($input_parameters = null)
    {
        if (empty($input_parameters) && !empty($this->bindParams)) {
            $input_parameters = $this->bindParams;
        }
        $timeout = isset($input_parameters['__timeout__']) ? $input_parameters['__timeout__'] : -1;
        $input_parameters = (array)$input_parameters;
        $this->result = $this->statement->execute($input_parameters, $timeout);
        $this->dbPool->freeConnect($this->connectId);
        if ($this->statement->errno != 0) {
            throw new \Exception($this->statement->error, $this->statement->errno);
        }
        return true;
    }

    public function fetchAll($how = null, $class_name = null, $ctor_args = null)
    {
        return $this->result;
    }
}