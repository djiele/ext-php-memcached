<?php

namespace Djiele\Memcached;

use Djiele\Memcached\Compressor\FastlzCompressor;
use Djiele\Memcached\Compressor\GzCompressor;
use Djiele\Memcached\Serializer\IgbinarySerializer;
use Djiele\Memcached\Serializer\JsonSerializer;
use Djiele\Memcached\Serializer\MsgpackSerializer;
use Djiele\Memcached\Serializer\PhpSerializer;

class MemcachedClient
{

    const DEFAULT_READ_LEN = 8192;

    protected $host = null;
    protected $port = null;
    protected $conn = null;
    protected $serializer = null;
    protected $compressor = null;

    protected $lastErrNo = 0;
    protected $lastErrMsg = '';
    protected $maxAllowableMem = 512;
    protected $maxIntLen = 10;
    protected $options = [
        Memcached::OPT_NO_BLOCK => true,
        Memcached::OPT_TCP_NODELAY => false,
        Memcached::OPT_CONNECT_TIMEOUT => 10,
        Memcached::OPT_COMPRESSION => true,
        Memcached::OPT_COMPRESSION_FACTOR => 1.3,
        Memcached::OPT_COMPRESSION_THRESHOLD => 2000,
        Memcached::OPT_COMPRESSION_TYPE => Memcached::COMPRESSION_FASTLZ,
        Memcached::OPT_SERIALIZER => Memcached::SERIALIZER_IGBINARY,
        Memcached::OPT_PREFIX_KEY => '',
    ];

    /**
     * MemcachedClient constructor.
     * @param $host
     * @param int $port
     * @param array $options
     */
    public function __construct($host, $port = 11211, array $options = [])
    {
        $this->maxIntLen = strlen((string)PHP_INT_MAX);
        $this->host = $host;
        $this->port = $port;
        if (0 == count($options)) {
            $this->setSerializer();
            $this->setCompressor();
        } else {
            $this->setOptions($options);
        }
    }

    /**
     * MemcachedClient destructor
     */
    public function __destruct()
    {
        if (is_resource($this->conn)) {
            fclose($this->conn);
        }
        $this->conn = null;
    }

    /**
     * Set configuration parameter
     * @param $key
     * @param $value
     */
    public function setOption($key, $value)
    {
        $this->options[$key] = $value;
        if (Memcached::OPT_COMPRESSION == $key) {
            $this->setCompressor();
        } elseif (Memcached::OPT_COMPRESSION_TYPE == $key) {
            $this->setCompressor();
        } elseif(Memcached::OPT_SERIALIZER) {
            $this->setSerializer();
        }
    }

    /**
     * Set batch configuration parameters
     * @param array $options
     */
    public function setOptions(array $options) {
        foreach ($options as $key => $value) {
            $this->setOption($key, $value);
        }
    }

    /**
     * Connect to memcache server
     * @return bool
     */
    public function connect()
    {
        if (is_resource($this->conn)) {
            $this->lastErrNo = Memcached::RES_SUCCESS;
            $this->lastErrMsg = "Connected to {$this->host}:{$this->port}.";

            return true;
        }
        $this->conn = @fsockopen(
            $this->host,
            $this->port,
            $this->lastErrNo,
            $this->lastErrMsg,
            $this->options[Memcached::OPT_CONNECT_TIMEOUT]
        );


       if (is_resource($this->conn)) {
           stream_set_blocking ($this->conn, $this->options[Memcached::OPT_NO_BLOCK]);
           stream_context_set_option (
               $this->conn,
               ['socket' => ['tcp_nodelay' => $this->options[Memcached::OPT_TCP_NODELAY]]]
           );
           $this->lastErrNo = Memcached::RES_SUCCESS;;
           $this->lastErrMsg = "Connected to {$this->host}:{$this->port}.";

           return true;
       }  else {
           switch ($this->lastErrNo) {
               case SOCKET_ECONNREFUSED:
                   $this->lastErrNo = MEMCACHED::RES_CONNECTION_SOCKET_CREATE_FAILURE;
                   $this->lastErrMsg = "Connection to {$this->host}:{$this->port} failed.";
                   break;
               case SOCKET_ETIMEDOUT:
                   $this->lastErrNo = MEMCACHED::RES_TIMEOUT;
                   $this->lastErrMsg = "Connection to {$this->host}:{$this->port} timed out.";
                   break;
               case SOCKET_EHOSTUNREACH:
                   $this->lastErrNo = MEMCACHED::RES_HOST_LOOKUP_FAILURE;
                   $this->lastErrMsg = "No route to {$this->host}:{$this->port}.";
                   break;
               default:
                   $this->lastErrMsg = "unknown error {$this->lastErrNo}. Connection to {$this->host}:{$this->port} failed.";
           }
           trigger_error("Connection to {$this->host}:{$this->port} failed.", E_USER_NOTICE);

           return false;
       }
    }

    /**
     * Check whether the server send an error response
     * @param $message
     * @return bool
     */
    protected function isError($message)
    {
        if (0 === strpos($message, 'CLIENT_ERROR')) {
            $this->lastErrNo = Memcached::RES_CLIENT_ERROR;
            $this->lastErrMsg = "{$message} on server {$this->host}:{$this->port}.";
            return true;
        }

        if (0 === strpos($message, 'SERVER_ERROR ')) {
            $this->lastErrNo = Memcached::RES_SERVER_ERROR;
            $this->lastErrMsg = "{$message} on server {$this->host}:{$this->port}.";
            return true;
        }

        if (0 === strpos($message, 'ERROR')) {
            $this->lastErrNo = Memcached::RES_SOME_ERRORS;
            $this->lastErrMsg = "{$message} on server {$this->host}:{$this->port}.";
            return true;
        }

        return false;
    }

    /**
     * Generic method for storage commands
     * @param $commandName
     * @param $key
     * @param $value
     * @param $exp
     * @param null $cas
     * @param bool $noreply
     * @return bool
     */
    protected function storageCommand($commandName, $key, $value, $exp, $cas = null, $noreply = false)
    {
        if (false === $this->connect()) {

            return false;
        }
        $commandName = strtolower($commandName);
        $key = $this->options[Memcached::OPT_PREFIX_KEY] . $key;
        $flag = $this->flag($value);
        if ($flag & 128) {
            $value = $this->serializer->serialize($value);
        }
        if (null !== $this->compressor && ($flag & 256)) {
            $tmp = $this->compressor->compress($value);
            if(strlen($value) > strlen($tmp) * $this->options[Memcached::OPT_COMPRESSION_FACTOR]) {
                $value = $tmp;
                unset($tmp);
            } else {
                $flag -= 256;
            }
        }
        $bytes = strlen((string)$value);
        if (false === $noreply) {
            $noreply = '';
        } else {
            $noreply = 'noreply';
        }
        if (in_array($commandName, ['set', 'add', 'replace', 'prepend', 'append'])) {
            $command = "{$commandName} {$key} {$flag} {$exp} {$bytes} {$noreply}";
            $expectAnswer = 'STORED';
        } elseif ('cas' == $commandName) {
            $command = "{$commandName} {$key} {$flag} {$exp} {$bytes} {$cas} {$noreply}";
            $expectAnswer = 'STORED';
        } else if ('delete' == $commandName) {
            $command = "{$commandName} {$key} {$noreply}";
            $expectAnswer = 'DELETED';
        } elseif ('touch' == $commandName) {
            $command = "{$commandName} {$key} {$exp} {$noreply}";
            $expectAnswer = 'TOUCHED';
        } else {
            $this->lastErrNo = Memcached::RES_PROTOCOL_ERROR;
            $this->lastErrMsg = "unknown command [{$commandName}].";
            return false;
        }
        $command = rtrim($command);
        if (in_array($commandName, ['delete', 'touch'])) {
            $command .= "\r\n";
        } else {
            $command .= "\r\n{$value}\r\n";
        }
        //echo $command;
        $commandLen = strlen($command);
        if ($commandLen == fwrite($this->conn, $command, $commandLen)) {
            if (false !== ($response = fgets($this->conn, 512))) {
                $response = rtrim($response);
                if ($this->isError($response)) {
                    return false;
                }
                if ($expectAnswer == $response) {
                    $this->lastErrNo = Memcached::RES_SUCCESS;
                    $this->lastErrMsg = '';
                    return true;
                } else {
                    switch($expectAnswer) {
                        case 'STORED':
                            if('add' == $commandName) {
                                $this->lastErrNo = Memcached::RES_DATA_EXISTS;
                            } else {
                                $this->lastErrNo = Memcached::RES_NOTSTORED;
                            }
                            break;
                        case 'DELETED':
                        case 'TOUCHED':
                            $this->lastErrNo = Memcached::RES_NOTFOUND;
                            break;
                    }
                    $this->lastErrMsg = "got [{$response}] response from server {$this->host}:{$this->port}.";
                }
            } else {
                $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                $this->lastErrMsg = "empty response from server {$this->host}:{$this->port}.";
            }
        } else {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
        }
        return false;
    }

    /**
     * Generic method for retrieval commands
     * @param $commandName
     * @param $key
     * @param null $value
     * @param null $exp
     * @param bool $rawValue
     * @return array|bool|mixed
     */
    protected function retrievalCommand($commandName, $key, $value = null, $exp = null, $rawValue = false)
    {
        if (false === $this->connect()) {

            return false;
        }
        $ret = [];
        $commandName = strtolower($commandName);
        if (is_array($key)) {
            $key = $this->options[Memcached::OPT_PREFIX_KEY]  .
                join(' ' . $this->options[Memcached::OPT_PREFIX_KEY], $key);
        } elseif(null !== $key) {
            $key = $this->options[Memcached::OPT_PREFIX_KEY] . $key;
        } else {
            $key = '';
        }
        if (null === $value) {
            $value = '';
        }
        if (in_array($commandName, ['gat', 'gats'])) {
            $command = "{$commandName} {$exp} {$key}";
            $expected = 'VALUE';
        } else {
            $command = "{$commandName} {$key} {$value}";
            if ('stats' == $commandName) {
                $expected = 'STAT';
            } else {
                $expected = 'VALUE';
            }
        }
        $command = rtrim($command) . "\r\n";
        //echo $command;
        $commandLen = strlen($command);
        if ($commandLen == fwrite($this->conn, $command, $commandLen)) {
            if (in_array($commandName, ['decr', 'incr'])) {
                $response = rtrim(fgets($this->conn, $this->maxIntLen + 2));
                if ($this->isError($response)) {

                    return false;
                } elseif ('NOT_FOUND' == $response) {
                    $this->lastErrNo = Memcached::RES_NOTFOUND;
                    $this->lastErrMsg = "got [{$response}] response from server {$this->host}:{$this->port}.";
                } else {
                    $ret = [$response];
                }
            } else {
                $nothingFound = true;
                while (false !== ($response = fgets($this->conn, 512)) && !feof($this->conn)) {
                    if ($this->isError($response)) {

                        return false;
                    }
                    if ("END\r\n" == $response) {
                        break;
                    }
                    if (0 === strpos($response, $expected . ' ')) {
                        $nothingFound = false;
                        $response = rtrim($response);
                        $metaTokens = explode(' ', $response);
                        array_shift($metaTokens);
                        $key_name = array_shift($metaTokens);
                        if (0 < ($lenPrefix = strlen($this->options[Memcached::OPT_PREFIX_KEY])) &&
                            0 === strpos($key_name, $this->options[Memcached::OPT_PREFIX_KEY])
                        ) {
                            $key_name = substr($key_name, $lenPrefix);
                        }
                        if ('VALUE' == $expected) {
                            $flag = array_shift($metaTokens);
                            $chunkLen = array_shift($metaTokens);
                            $cas = array_shift($metaTokens);
                            $returnValue = fread($this->conn, $chunkLen);
                            if ($chunkLen > ($retValLen = strlen($returnValue))) {
                                if(self::DEFAULT_READ_LEN <= ($n = ($chunkLen - $retValLen))) {
                                    $readLen = self::DEFAULT_READ_LEN;
                                } else {
                                    $readLen = $n;
                                }
                                do {
                                    $chunk = fread($this->conn, $readLen);
                                    if (false === $chunk) {
                                        break;
                                    }
                                    $returnValue .= $chunk;
                                    $retValLen = strlen($returnValue);
                                    if(self::DEFAULT_READ_LEN <= ($n = ($chunkLen - $retValLen))) {
                                        $readLen = self::DEFAULT_READ_LEN;
                                    } else {
                                        $readLen = $n;
                                    }
                                } while($chunkLen > $retValLen);
                            }
                            if (false === $rawValue) {
                                $returnValue = $this->setType($returnValue, $flag);
                            }
                            $ret[] = [
                                'key' => $key_name,
                                'value' => $returnValue,
                                'cas' => $cas,
                                'flags' => $flag,
                            ];
                        } else {
                            $ret[] = [
                                'key' => $key_name, 'value' => array_shift($metaTokens),
                            ];
                        }
                    }
                }
            }
        } else {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
        }
        if (in_array($commandName, ['incr', 'decr'])) {
            $ret = current($ret);
        }
        if (isset($nothingFound) && true === $nothingFound) {
            $this->lastErrNo = Memcached::RES_NOTFOUND;
            $this->lastErrMsg = "key [{$key}] not found on server {$this->host}:{$this->port}.";
        }
        return $ret;
    }

    /**
     * Store given key only if it doesn't exist
     * @param $key
     * @param $value
     * @param $exp
     * @param bool $noreply
     * @return bool
     */
    public function add($key, $value, $exp, $noreply = false)
    {
        return $this->storageCommand('add', $key, $value, $exp, $noreply);
    }

    /**
     * Store given key
     * @param $key
     * @param $value
     * @param $exp
     * @param bool $noreply
     * @return bool
     */
    public function set($key, $value, $exp, $noreply = false)
    {
        return $this->storageCommand('set', $key, $value, $exp, $noreply);
    }

    /**
     * Replace value of an existing given key, expiration time is unchanged
     * @param $key
     * @param $value
     * @param $exp
     * @param bool $noreply
     * @return bool
     */
    public function replace($key, $value, $exp, $noreply = false)
    {
        return $this->storageCommand('replace', $key, $value, $exp, $noreply);
    }

    /**
     * Add value before existing data for given key
     * @param $key
     * @param $value
     * @param bool $noreply
     * @return bool
     */
    public function prepend($key, $value, $noreply = false)
    {
        return $this->storageCommand('prepend', $key, $value, 0, $noreply);
    }

    /**
     * Add value to existing data of given key
     * @param $key
     * @param $value
     * @param bool $noreply
     * @return bool
     */
    public function append($key, $value, $noreply = false)
    {
        return $this->storageCommand('append', $key, $value, 0, $noreply);
    }

    /**
     * Store given key only if no one else has updated it since I last fetched it
     * @param $key
     * @param $value
     * @param $exp
     * @param $cas
     * @param bool $noreply
     * @return bool
     */
    public function cas($key, $value, $exp, $cas, $noreply = false)
    {
        return $this->storageCommand('cas', $key, $value, $exp, $cas, $noreply);
    }

    /**
     * Modify expiration time of given key without fetching it
     * @param $key
     * @param $exp
     * @param bool $noreply
     * @return bool
     */
    public function touch($key, $exp, $noreply = false)
    {
        return $this->storageCommand('touch', $key, null, $exp, null, $noreply);
    }

    /**
     * Delete given key
     * @param $key
     * @param bool $noreply
     * @return bool
     */
    public function delete($key, $noreply = false)
    {
        return $this->storageCommand('delete', $key, null, null, null, $noreply);
    }

    /**
     * Get value of given key
     * @param $key
     * @param bool $rawValue
     * @return array|bool|mixed
     */
    public function get($key, $rawValue = false)
    {
        return $this->retrievalCommand('get', $key, null, null, $rawValue);
    }

    /**
     * Get value of multiple keys
     * @param array $keys
     * @param bool $rawValue
     * @return array|bool|mixed
     */
    public function gets(array $keys, $rawValue = false)
    {
        return $this->retrievalCommand('gets', $keys, null, null, $rawValue);
    }

    /**
     * Modify expiration time of given key and fetch its value
     * @param $key
     * @param $exp
     * @param bool $rawValue
     * @return array|bool|mixed
     */
    public function gat($key, $exp, $rawValue = false)
    {
        return $this->retrievalCommand('gat', $key, null, $exp, $rawValue);
    }

    /**
     * Modify expiration time of multiple keys and fetch their value
     * @param array $keys
     * @param $exp
     * @param bool $rawValue
     * @return array|bool|mixed
     */
    public function gats(array $keys, $exp, $rawValue = false)
    {
        return $this->retrievalCommand('gats', $keys, null, $exp, $rawValue);
    }

    /**
     * Return all keys currently held on servers
     * @return array|int
     */
    public function getAllKeysWithCacheDump()
    {
        if (false === $this->connect()) {

            return false;
        }
        $command = "stats items\r\n" . 'stats cachedump $slabindex $count' . "\r\n";
        // retrieve distinct slab
        $r = @fwrite($this->conn, "stats items\r\n");
        if ($r === false) {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
            return $this->lastErrNo;
        }

        $slab = [];
        while (($l = @fgets($this->conn, 1024)) !== false) {
            // finished?
            $l = trim($l);
            if ($l == 'END') {
                break;
            }

            $m = [];
            // <STAT items:1:number 2>
            $r = preg_match('/^STAT\sitems\:(\d+)\:number\s(\d+)/', $l, $m);
            if ($r != 1) {
                continue;
            }
            $slab[$m[1]] = $m[2];
        }
        reset($slab);
        foreach ($slab as $a_slab_key => &$a_slab) {
            $r = @fwrite($this->conn, 'stats cachedump ' . $a_slab_key . " ". $a_slab ."\r\n");
            $a_slab = [];
            if ($r === false) {
                $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
                $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
                return $this->lastErrNo;
            }

            while (($l = @fgets($this->conn, 1024)) !== false) {
                // finished?
                $l = trim($l);
                if ($l == 'END') {
                    break;
                }

                $m = [];
                // ITEM 42 [118 b; 1354717302 s]
                $r = preg_match('/^ITEM\s([^\s]+)\s/', $l, $m);
                if ($r != 1) {
                    continue;
                }
                $a_key = $m[1];

                $a_slab[] = $a_key;
            }
        }

        $keys = [];
        reset($slab);
        foreach ($slab AS &$a_slab) {
            reset($a_slab);
            foreach ($a_slab AS &$a_key) {
                $keys[] = $a_key;
            }
        }
        unset($slab);

        return $keys;
    }

    /**
     * Return all keys currently held on servers
     * @return array
     */
    function getAllKeysWithLruCrawler()
    {
        if (false === $this->connect()) {

            return false;
        }
        $ret = [];
        $command = "lru_crawler metadump all\r\n";
        //echo $command;
        $written = fwrite($this->conn, $command, strlen($command));
        if(false === $written) {
            $ret = [];
        } else {
            while (false !== ($row = rtrim(fgets($this->conn, 512)))) {
                if ('END' == $row) {
                    break;
                }
                $row = substr($row, 0, strpos($row, ' '));
                $row = substr($row, strpos($row, '=') + 1);
                $ret[] = $row;
            }
        }
        return $ret;
    }

    /**
     * Increment value of given integer key
     * @param $key
     * @param $value
     * @return array|bool|mixed
     */
    public function incr($key, $value)
    {
        return $this->retrievalCommand('incr', $key, $value);
    }

    /**
     * Decrement value of given integer key
     * @param $key
     * @param $value
     * @return array|bool|mixed
     */
    public function decr($key, $value)
    {
        return $this->retrievalCommand('decr', $key, $value);
    }

    /**
     * Get various statistics depending on provided type (null, settings, items, sizes, slabs, conns)
     * @param null $type
     * @return array|bool|mixed
     */
    public function stats($type = null)
    {
        return $this->retrievalCommand('stats', $type, null, null);
    }

    /**
     * Get server version
     * @return bool|string
     */
    public function version()
    {
        if (false === $this->connect()) {

            return false;
        }
        $ret = '';
        $command = "version\r\n";
        //echo $command;
        $written = fwrite($this->conn, $command, strlen($command));
        if(false == $written) {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
            $ret = false;
        } else {
            $ret = fgets($this->conn, 512);
            if(false === $ret) {
                $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                $this->lastErrMsg = "reading from {$this->host}:{$this->port} failed.";
                $ret = false;
            } else {
                $ret = rtrim(substr($ret, strpos($ret, ' ') + 1));
            }
        }

        return $ret;
    }

    /**
     * Invalidate all existing items immediately (by default) or after the expiration specified
     * @param null $exp
     * @param bool $noreply
     * @return bool
     */
    public function flush_all($exp = null, $noreply = false)
    {
        if (false === $this->connect()) {

            return false;
        }
        $ret = '';
        if (null === $exp) {
            $exp = '';
        }
        if (false === $noreply) {
            $noreply = '';
        } else {
            $noreply = 'noreply';
        }
        $command = rtrim(preg_replace('/\s+/', ' ', "flush_all {$exp} {$noreply}")) . "\r\n";
        //echo $command;
        $written = fwrite($this->conn, $command, strlen($command));
        if(false == $written) {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
            $ret = 'KO';
        } else {
            $ret = fgets($this->conn, 512);
            if(false === $ret) {
                $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                $this->lastErrMsg = "reading from {$this->host}:{$this->port} failed.";
                $ret = 'KO';
            }
        }
        return $ret == "OK\r\n";
    }

    /**
     * Allows runtime adjustments of the cache memory limit
     * @param $size
     * @param bool $noreply
     * @return bool
     */
    public function cache_memlimit($size, $noreply = false)
    {
        if (false === $this->connect()) {

            return false;
        }
        $ret = '';
        if (false === $noreply) {
            $noreply = '';
        } else {
            $noreply = 'noreply';
        }
        $size = min($size, $this->maxAllowableMem);
        $command = rtrim(preg_replace('/\s+/', ' ', "cache_memlimit {$size} {$noreply}")) . "\r\n";
        //echo $command;
        $written = fwrite($this->conn, $command, strlen($command));
        if(false == $written) {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
            $ret = 'KO';
        } else {
            $ret = fgets($this->conn, 512);
            if(false === $ret) {
                $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                $this->lastErrMsg = "reading from {$this->host}:{$this->port} failed.";
                $ret = 'KO';
            }
        }
        return $ret == "OK\r\n";
    }

    /**
     * Set the verbosity level of the logging output
     * @param $level
     * @param bool $noreply
     * @return bool
     */
    public function verbosity($level, $noreply = false)
    {
        if (false === $this->connect()) {

            return false;
        }
        $ret = '';
        if (false === $noreply) {
            $noreply = '';
        } else {
            $noreply = 'noreply';
        }
        $size = max(min($level, 3), 0);
        $command = rtrim(preg_replace('/\s+/', ' ', "verbosity {$size} {$noreply}")) . "\r\n";
        //echo $command;
        $written = fwrite($this->conn, $command, strlen($command));
        if(false == $written) {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
            $ret = 'KO';
        } else {
            $ret = fgets($this->conn, 512);
            if(false === $ret) {
                $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                $this->lastErrMsg = "reading from {$this->host}:{$this->port} failed.";
                $ret = 'KO';
            }
        }
        return $ret == "OK\r\n";
    }

    /**
     * Upon receiving this command, the server closes the connection
     * @return true|string
     */
    public function quit()
    {
        $ret = true;
        if (is_resource($this->conn)) {
            $command = "quit\r\n";
            //echo $command;
            $written = fwrite($this->conn, $command, strlen($command));
            if(false == $written) {
                $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
                $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
                $ret = false;
            } else {
                if (false === ($ret = fgets($this->conn, 512))) {
                    $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                    $this->lastErrMsg = "reading from  {$this->host}:{$this->port} failed.";
                    $ret = false;
                } else {
                    $ret = substr($ret, strpos($ret, ' ') + 1);
                }
            }
            fclose($this->conn);
        }
        $this->conn = null;
        return $ret;
    }

    /**
     * Get the last error code
     * @return int
     */
    public function getLastErrNo()
    {
        return $this->lastErrNo;
    }

    /**
     * Get the last error message
     * @return string
     */
    public function getLastErrMsg()
    {
        return $this->lastErrMsg;
    }

    /**
     * Set option serializer to be used
     */
    protected function setSerializer()
    {
        switch($this->options[Memcached::OPT_SERIALIZER]) {
            case Memcached::SERIALIZER_MSGPACK:
                $this->serializer = new MsgpackSerializer();
                break;
            case Memcached::SERIALIZER_PHP:
                $this->serializer = new PhpSerializer();
                break;
            case Memcached::SERIALIZER_JSON:
                $this->serializer = new JsonSerializer();
                break;
            default:
                $this->serializer = new IgbinarySerializer();
        }

    }

    /**
     * Set option compressor to be used
     */
    protected function setCompressor()
    {
        if (true === $this->options[Memcached::OPT_COMPRESSION]) {
            switch($this->options[Memcached::OPT_COMPRESSION_TYPE]) {
                case Memcached::COMPRESSION_GZ:
                    $this->compressor = new GzCompressor();
                    break;
                default:
                    $this->compressor = new FastlzCompressor();
            }
        } else {
            $this->compressor = null;
        }
    }

    /**
     * Get the flag corresponding to the given value type
     * @param $value
     * @return int
     */
    protected function flag($value)
    {
        $v = 0;
        $v += is_string($value) ? 1 : 0;
        $v += is_int($value) ? 2 : 0;
        $v += is_float($value) ? 4 : 0;
        $v += (true === $value || false === $value) ? 8 : 0;
        $v += null === $value ? 16 : 0;
        $v += is_array($value) ? 32 : 0;
        $v += is_object($value) ? 64 : 0;
        $v += (32 == $v || 64 == $v) ? 128 : 0; // serializable
        $condLenString = 1 == $v && $this->options[Memcached::OPT_COMPRESSION_THRESHOLD] < strlen($value);
        $v += (($v & 128) == 128) || $condLenString? 256 : 0; // long string, array or object can be compressed
        return $v;
    }

    /**
     * Restore original type from stored value
     * @param $value
     * @param $flag
     * @return mixed
     */
    protected function setType($value, $flag)
    {
        $types = [
            1 => 'string', 2 => 'integer', 4 => 'float', 8 => 'boolean',
            16 => 'NULL', 32 => 'array', 64 => 'object',
        ];

        if (null !== $this->compressor && ($flag & 256)) {
            $value = $this->compressor->decompress($value);
        }
        if (null !== $this->serializer && ($flag & 128)) {
            $value = $this->serializer->deserialize($value);
        } else {
            foreach ($types as $typeNo => $typeStr) {
                if ($flag & $typeNo) {
                    settype($value, $typeStr);
                }
            }
        }
        return $value;
    }
}
