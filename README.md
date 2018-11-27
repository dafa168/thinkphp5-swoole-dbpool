基于tp5的swoole支持

对th5的connection进行改造，使用Swoole\Coroutine\MySQL重写了基于swoole的PDO接口，实现了mysql的数据库连接池，本地测试可用。
使用时，替换thinkphp/library/think/db/Connection.php，

并拷贝SwoolePDO.php，SwoolePDOStatement.php，SwooleMySQL.php到 thinkphp/library/think/db文件夹下。

支持单事务