<?php
if (!defined('__TYPECHO_ROOT_DIR__')) exit;

class Typecho_Db_Adapter_Mssql implements Typecho_Db_Adapter
{
    private $_dbConn;

    public static function isAvailable()
    {
        return function_exists('sqlsrv_connect');
    }

    public function connect(Typecho_Config $config)
    {
        $serverName = 'tcp:{$config->host},{$config->port}';
        $connectionOptions = array(
            "Database"=>$config->database,
            "Uid"=>$config->user,
            "PWD"=>$config->password);

        $this->$_dbConn = @sqlsrv_connect($serverName, $connectionOptions);
        
        if($this->$_dbConn == false)
            throw new Typecho_Db_Adapter_Exception(print_r(@sqlsrv_errors()));            
        else
            return $this->$_dbConn;
    }

    public function getVersion($handle)
    {
        $serverInfo = @sqlsrv_server_info( $handle);  
        if( $serverInfo )  
            return 'ext:mssql' . $serverInfo['SQLServerVersion'];
        return 'ext:mssql'
    }

    public function query($query, $handle, $op = Typecho_Db::READ, $action = NULL)
    {
        if ($resource = @sqlsrv_query($handle,$query instanceof Typecho_Db_Query ? $query->__toString() : $query)) {
            return $resource;
        }

        throw new Typecho_Db_Query_Exception(print_r(@sqlsrv_errors()));
    }

    public function fetch($resource)
    {
        return sqlsrv_fetch_array($resource);
    }

    public function fetchObject($resource)
    {
        return sqlsrv_fetch_object($resource);
    }

    public function quoteValue($string)
    {
        return '\'' . str_replace(array('\'', '\\'), array('\'\'', '\\\\'), $string) . '\'';
    }

    public function quoteColumn($string)
    {
        return '"' . $string . '"';
    }

    public function parseSelect(array $sql)
    {
        if (!empty($sql['join'])) {
            foreach ($sql['join'] as $val) {
                list($table, $condition, $op) = $val;
                $sql['table'] = "{$sql['table']} {$op} JOIN {$table} ON {$condition}";
            }
        }


        if($sql['order'] != NULL)
        {
            $sql['limit'] = (0 == strlen($sql['limit'])) ? NULL : ' FETCH NEXT ' . $sql['limit'] . ' ROWS ONLY ';
            $sql['offset'] = (0 == strlen($sql['offset'])) ? NULL : ' OFFSET ' . $sql['offset'] . ' ROWS ';
            return 'SELECT ' . $sql['fields'] . ' FROM ' . $sql['table'] .
            $sql['where'] . $sql['group'] . $sql['having'] . $sql['order']. $sql['offset'] . $sql['limit'] ;    
        }
        else if(0 != strlen($sql['limit']))
        {
            $sql['limit'] = ' TOP ' . ($sql['limit'] + (0 == strlen($sql['offset']) ? 0 : $sql['offset']));
            $sql['offset'] = ' TOP ' . (0 == strlen($sql['offset']) ? 0 : $sql['offset']);

            return 'SELECT ' . $sql['limit'] . $sql['fields'] . ' FROM ' . $sql['table'] .
            $sql['where'] . $sql['group'] . $sql['having'] . $sql['order'] . 
            ' EXCEPT ' .
            'SELECT ' . $sql['limit'] . $sql['fields'] . ' FROM ' . $sql['table'] .
            $sql['where'] . $sql['group'] . $sql['having'] . $sql['order'];
    
        }
        return 'SELECT ' . $sql['fields'] . ' FROM ' . $sql['table'] .
        $sql['where'] . $sql['group'] . $sql['having'] . $sql['order'];
    }

    public function affectedRows($resource, $handle)
    {
        return sqlsrv_rows_affected($handle);
    }

    public function lastInsertId($resource, $handle)
    {
        //return mysql_insert_id($handle);
        return 0;
    }
}
?>