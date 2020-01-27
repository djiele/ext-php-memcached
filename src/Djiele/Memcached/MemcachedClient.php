<?php

namespace Djiele\Memcached;

use Djiele\Memcached\Exception\CommandNotFoundException;
use Djiele\Memcached\Exception\SocketReadException;
use Djiele\Memcached\Compressor\FastlzCompressor;
use Djiele\Memcached\Compressor\GzCompressor;
use Djiele\Memcached\Serializer\IgbinarySerializer;
use Djiele\Memcached\Serializer\JsonSerializer;
use Djiele\Memcached\Serializer\MsgpackSerializer;
use Djiele\Memcached\Serializer\PhpSerializer;
use Djiele\Memcached\Sasl\SaslAuthDigestMd5;

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
        Memcached::OPT_BINARY_PROTOCOL => false,
        Memcached::OPT_NO_BLOCK => true,
        Memcached::OPT_TCP_NODELAY => false,
        Memcached::OPT_CONNECT_TIMEOUT => 10,
        Memcached::OPT_COMPRESSION => true,
        Memcached::OPT_COMPRESSION_FACTOR => 1.3,
        Memcached::OPT_COMPRESSION_THRESHOLD => 2000,
        Memcached::OPT_COMPRESSION_TYPE => Memcached::COMPRESSION_FASTLZ,
        Memcached::OPT_SERIALIZER => Memcached::SERIALIZER_IGBINARY,
        Memcached::OPT_SASL_AUTH_METHOD => Memcached::SASL_AUTH_NONE,
        Memcached::OPT_PREFIX_KEY => '',
    ];
    protected $serverVersion = null;
    protected $saslMechsList = null;
    protected $username = null;
    protected $password = null;
    protected $authenticated = false;
    protected $authenticating = false;

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
     * Open TCP connection to memcache server
     * @return mixed resource|bool
     */
    protected function tcpConnection() {
        if (is_resource($this->conn)) {
            $this->lastErrNo = Memcached::RES_SUCCESS;
            $this->lastErrMsg = "Connected to {$this->host}:{$this->port}.";

            return $this->conn;
        }
        $this->conn = @fsockopen(
            $this->host,
            $this->port,
            $this->lastErrNo,
            $this->lastErrMsg,
            $this->options[Memcached::OPT_CONNECT_TIMEOUT]
        );

       if (is_resource($this->conn)) {
            stream_set_blocking ($this->conn, $this->options[Memcached::OPT_NO_BLOCK] ? false : true);
            stream_context_set_option(
                $this->conn,
                ['socket' => ['tcp_nodelay' => $this->options[Memcached::OPT_TCP_NODELAY]]]
            );
            $this->lastErrNo = Memcached::RES_SUCCESS;
            $this->lastErrMsg = "Connected to {$this->host}:{$this->port}.";

            return $this->conn;
        } else {
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
            trigger_error("Connection to {$this->host}:{$this->port} failed.", E_USER_WARNING);

            return false;
       }
    }

    /**
     * Connect to memcache server
     * @param bool $skipAuth skip authentication process for commands that don't need it
     * @return bool
     */
    public function connect($skipAuth = false)
    {
        if(is_resource($ret = $this->tcpConnection())) {
            if (true === $this->needAuth()) {
                if (false === $skipAuth && true === $this->hasCredentials()) {
                    if (! $this->authenticating) {
                        $this->saslAuth();
                        if (false === $this->authenticated) {
                            fclose($ret);
                            
                            return false;
                        }
                    }
                }
            }
            
            return true;
        }
        
        return false;
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
     * Generic method for storage commands in ASCII protocol
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
        list($flag, $value) = $this->prepareStoredData($value);
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
     * Generic method for retrieval commands in ASCII protocol
     * @param $commandName
     * @param $key
     * @param null $value
     * @param null $exp
     * @return array|bool|mixed
     */
    protected function retrievalCommand($commandName, $key, $value = null, $exp = null)
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
                            $ret[] = [
                                'key' => $key_name,
                                'value' => $this->setType($returnValue, $flag),
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
     * Generic method for simple commands in ASCII protocol
     * @param resource $connection
     * @param string $command
     * @return bool|mixed
     */
    protected function executeSimpleCommand($connection, $command)
    {
        if (false === $this->connect()) {

            return false;
        }
        
        if ("\r\n" != substr($command, -2,2)) {
            $command .= "\r\n";
        }
        $written = fwrite($connection, $command, strlen($command));
        if(false === $written) {
            $this->lastErrNo = Memcached::RES_WRITE_FAILURE;
            $this->lastErrMsg = "writing {$this->host}:{$this->port} failed.";
            $ret = false;
        } else {
            if (false === ($ret = fgets($connection, 512))) {
                $this->lastErrNo = Memcached::RES_UNKNOWN_READ_FAILURE;
                $this->lastErrMsg = "reading from  {$this->host}:{$this->port} failed.";
            }
            if ($this->isError($ret)) {
                $ret = false;
            }
        }
        
        return $ret;
    }

    /**
     * Generic method for storage commands in binary protocol
     * @param $opcode
     * @param $key
     * @param $value
     * @param $exp
     * @return bool
     */
    protected function storageCommandBinary($opcode, $key, $value, $exp = null)
    {
        if (false === $this->connect()) {

            return false;
        }

        if (in_array($opcode, [0x0e, 0x19, 0x0f, 0x1a])) {
            list($flags, $value) = $this->prepareStoredData($value);
            $v = current($this->get($key));
            if(1 != $v['flags'] || 1 !== $flags) {
                return false;
            }
            $request = [
                    'opcode' => $opcode,
                    'key' => $key,
                    'value' => $value,
            ];
        } elseif (in_array($opcode, [0x1c, 0x04, 0x14])) {
            $request = [
                    'opcode' => $opcode,
                    'key' => $key,
                    'extra' => $exp,
            ];
        } elseif (in_array($opcode, [0x18, 0x08])) {
            $request = [
                    'opcode' => $opcode,
                    'extra' => $exp,
            ];
        } else {
            list($flags, $value) = $this->prepareStoredData($value);
            $request = [
                    'opcode' => $opcode,
                    'key' => $key,
                    'value' => $value,
                    'extra' =>  pack('NN', $flags, $exp),
            ];
        }
        $this->binRequest($request);
        $data = $this->binResponse();

        return Memcached::RES_SUCCESS === $this->lastErrNo;
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
        if (true === $this->isBinaryProtocol()) {
            
            return $this->storageCommandBinary(($noreply ? 0x12 : 0x02), $key, $value, $exp);
        } else {
            
            return $this->storageCommand('add', $key, $value, $exp, $noreply);
        }
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
        if (true === $this->isBinaryProtocol()) {
            
            return $this->storageCommandBinary(($noreply ? 0x11 : 0x01), $key, $value, $exp);
        } else {
            
            return $this->storageCommand('set', $key, $value, $exp, $noreply);
        }
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
        if (true === $this->isBinaryProtocol()) {
            
            return $this->storageCommandBinary(($noreply ? 0x13 : 0x03), $key, $value, $exp);
        } else {
            
            return $this->storageCommand('replace', $key, $value, $exp, $noreply);
        }
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
        if (true === $this->isBinaryProtocol()) {
            
            return $this->storageCommandBinary(($noreply ? 0x1a : 0x0f), $key, $value);
        } else {
            
            return $this->storageCommand('prepend', $key, $value, 0, $noreply);
        }
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
        if (true === $this->isBinaryProtocol()) {
            
            return $this->storageCommandBinary(($noreply ? 0x19 : 0x0e), $key, $value);
        } else {
            
            return $this->storageCommand('append', $key, $value, 0, $noreply);
        }
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
        if (true === $this->isBinaryProtocol()) {
            $v = current($this->get($key));
            if($cas == $v['cas']) {
                return $this->set($key, $value, $exp, $noreply);
            } else {
                return false;
            }
        } else {
            
            return $this->storageCommand('cas', $key, $value, $exp, $cas, $noreply);
        }
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
        if (true === $this->isBinaryProtocol()) {
            
            return $this->storageCommandBinary(0x1c, $key, null, pack('N', $exp));
        } else {
            
            return $this->storageCommand('touch', $key, null, $exp, null, $noreply);
        }
    }

    /**
     * Delete given key
     * @param $key
     * @param bool $noreply
     * @return bool
     */
    public function delete($key, $noreply = false)
    {
        if (true === $this->isBinaryProtocol()) {
            return $this->storageCommandBinary(($noreply ? 0x14 : 0x04), $key, null, $exp);
        } else {
            return $this->storageCommand('delete', $key, null, null, null, $noreply);
        }
    }

    /**
     * Get value of given key
     * @param $key
     * @return array|bool|mixed
     */
    public function get($key)
    {
        if (true === $this->isBinaryProtocol()) {
            $this->binRequest(
                array(
                    'opcode' => 0x00,
                    'key' => $key,
                )
            );

            $data = $this->binResponse();
            if(Memcached::RES_SUCCESS === $this->lastErrNo) {
                return [[
                    'key'   => $key,
                    'value' => $this->settype($data['value'], $data['flags']),
                    'cas'   => $data['cas'],
                    'flags' => $data['flags'],
                ]];
            }
            
            return false;
        } else {
            return $this->retrievalCommand('get', $key, null, null);
        }
    }

    /**
     * Get value of multiple keys
     * @param array $keys
     * @return array|bool|mixed
     */
    public function gets(array $keys)
    {
        if (true === $this->isBinaryProtocol()) {
            $ret = [];
            $pipeline = array_fill(0, ($n=count($keys)), ['opcode' => 0x0d, 'key' => null]);
            for ($i=0; $i<$n; $i++) {
               $pipeline[$i]['key'] = $keys[$i];
               if ($i == $n-1) {
                   $pipeline[$i]['opcode'] = 0x0c;
               }
            }
            $this->binRequest($pipeline);
            for ($i=0; $i<$n; $i++) {
                $ret[] = $this->binResponse();
            }
            return $ret;
        } else {
            return $this->retrievalCommand('gets', $keys, null, null);
        }
    }

    /**
     * Modify expiration time of given key and fetch its value
     * @param array $key
     * @param $exp
     * @return array|bool|mixed
     */
    public function gat($key, $exp)
    {
        return $this->gats([$key], $exp);
    }
    /**
     * Modify expiration time of multiple keys and fetch their value
     * @param array $keys
     * @param $exp
     * @return array|bool|mixed
     */
    public function gats(array $keys, $exp)
    {
        if (true === $this->isBinaryProtocol()) {
            $pipeline = [];
            $n = count($keys);
            for ($i=0; $i<$n; $i++) {
                $pipeline[] = ['opcode' => ($i == $n-1 ? 0x0d : 0x0c), 'key' => $keys[$i]];
                $pipeline[] = ['opcode' => 0x1c, 'key' => $keys[$i], 'extra' => pack('N', $exp)];
            }
            $n = count($pipeline);
            $this->binRequest($pipeline);
            for ($i=0; $i<$n; $i++) {
                $tmp = $this->binResponse();
                if (!empty($tmp['key']) && 0<$tmp['cas']) {
                    $ret[] = $tmp;
                }
            }
            return $ret;
        } else {
            return $this->retrievalCommand('gats', $keys, null, $exp);
        }
    }

    /**
     * Increment value of given integer key
     * @param $key
     * @param $value
     * @return array|bool|mixed
     */
    public function incr($key, $value, $initialValue, $exp)
    {
        if (true === $this->isBinaryProtocol()) {
            $this->binRequest(
                [
                    'opcode' => 0x05,
                    'key' => $key,
                    'extra' => pack('JJN', $value, $initialValue, $exp),
                ]
            );

            $data = $this->binResponse();
            if(Memcached::RES_SUCCESS === $this->lastErrNo) {
                $rawResponse = unpack('Jvalue', $data['value']);
                return $this->settype($rawResponse['value'], 2);
            }
            
            return false;
        } else {
            return $this->retrievalCommand('incr', $key, $value);
        }
    }

    /**
     * Decrement value of given integer key
     * @param $key
     * @param $value
     * @return array|bool|mixed
     */
    public function decr($key, $value)
    {
        if (true === $this->isBinaryProtocol()) {
            $this->binRequest(
                [
                    'opcode' => 0x06,
                    'key' => $key,
                    'extra' => pack('JJN', $value, $initialValue, $exp),
                ]
            );

            $data = $this->binResponse();
            if(Memcached::RES_SUCCESS === $this->lastErrNo) {
                $rawResponse = unpack('Jvalue', $data['value']);
                return $this->settype($rawResponse['value'], 2);
            }
            
            return false;
        } else {
            return $this->retrievalCommand('decr', $key, $value);
        }
    }

    /**
     * Get various statistics depending on provided type (null, settings, items, sizes, slabs, conns)
     * @param null $type
     * @return array|bool|mixed
     */
    public function stats($type = null)
    {
        if (true === $this->isBinaryProtocol()) {
            $ret = [];
            $request = ['opcode' => 0x10];
            if(null !== $type && in_array($type, ['settings', 'items', 'sizes', 'slabs', 'conns'])) {
                $request['key'] = $type;
            }
            $this->binRequest($request);
            while(true) {
                $data = $this->binResponse();
                if('' == $data['key'].$data['value']) {
                    break;
                }
                $ret[] = ['key' => $data['key'], 'value' => $data['value']];
            }
            
            return $ret;
        } else {
            return $this->retrievalCommand('stats', $type, null, null);
        }
    }

    /**
     * Get server version
     * @return bool|string
     */
    public function version()
    {
        $ret = '';
        if (true === $this->isBinaryProtocol()) {
            $this->binRequest(['opcode' => 0x0b]);
            if(is_array($data = $this->binResponse()) && array_key_exists('value', $data)) {
                $ret = strval($data['value']);
            } else {
                return false;
            }
        } else {
            $ret = $this->executeSimpleCommand($this->conn, 'version');
            if (false !== $ret) {
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
    public function flush($exp = null, $noreply = false)
    {
        $ret = '';
        if (true === $this->isBinaryProtocol()) {
            
            if($this->storageCommandBinary(($noreply ? 0x18 : 0x08), null, null, pack('N', $exp))) {
                $ret = "OK\r\n";
            }
        } else {
            if (null === $exp) {
                $exp = '';
            }
            if (false === $noreply) {
                $noreply = '';
            } else {
                $noreply = 'noreply';
            }
            $command = rtrim(preg_replace('/\s+/', ' ', "flush_all {$exp} {$noreply}"));
            $ret = $this->executeSimpleCommand($this->conn, $command);
        }
        return $ret == "OK\r\n";
    }

    /**
     * Alias of method flush
     * Invalidate all existing items immediately (by default) or after the expiration specified
     * @param null $exp
     * @param bool $noreply
     * @return bool
     */
    public function flush_all($exp = null, $noreply = false)
    {
        return $this->flush($exp, $noreply);
    }
    
    /**
     * Get server authentication mechanisms
     * @return array
     */
    public function sasl_list_mechs()
    {
        if (is_array($this->saslMechsList)) {
            return $this->saslMechsList;
        }
        if (false === $this->connect(true)) {

            return false;
        }
        return $this->saslGetAndSetMechs();
    }
    
    protected function saslGetAndSetMechs() {
        $ret = null;
        if (true === $this->isBinaryProtocol()) {
            $this->binRequest(['opcode' => 0x20]);
            $data = $this->binResponse();
            $ret = explode(' ', $data['value']);
            $this->saslMechsList = $ret;
        }
        return $ret;
    }
    
    /**
     * Set auth credentials
     */
    public function sasl_auth($username, $password)
    {
        if (false === $this->options[Memcached::OPT_BINARY_PROTOCOL]) {
            trigger_error(
                'SASL authentication is only supported with binary protocol',
                E_USER_WARNING
            );
        }
        $this->username = $username;
        $this->password = $password;
        
        return true;
    }
    
    /**
     * SASL authentication
     * @return bool
     */
    protected function saslAuth()
    {
        $ret = false;
        $this->authenticating = true;
        if(false === $this->authenticated) {
            if (null === $this->saslMechsList) {
                $this->saslGetAndSetMechs();
            }
            $knownAlgos = [];
            foreach ($this->saslMechsList as $mech) {
                $mech = str_replace('-', '_', $mech);
                if($const = @constant(__NAMESPACE__ . '\Memcached::SASL_AUTH_' . $mech)) {
                    $knownAlgos[] = $const;
                }
            }
            if(in_array($this->options[Memcached::OPT_SASL_AUTH_METHOD], $knownAlgos)) {
                switch ($this->options[Memcached::OPT_SASL_AUTH_METHOD]) {
                    case Memcached::SASL_AUTH_DIGEST_MD5:
                        $ret = $this->saslAuthDigestMd5();
                        break;
                    case Memcached::SASL_AUTH_CRAM_MD5:
                        $ret = $this->saslAuthCramMd5();
                        break;
                    case Memcached::SASL_AUTH_LOGIN:
                        $ret = $this->saslAuthLogin();
                        break;
                    case Memcached::SASL_AUTH_PLAIN:
                        $ret = $this->saslAuthPlain();
                        break;
                    case Memcached::SASL_AUTH_ANONYMOUS:
                        $ret = $this->saslAuthAnonymous();
                        break;
                    default:
                        $ret = false;
                }
                if(true === $ret) {
                    $this->authenticated = true;
                }
            } else {
                trigger_error(
                    'selected authentication method is unsupported, valid options are [' . join(', ', $this->saslMechsList) . ']',
                    E_USER_ERROR
                );
            }
            $this->authenticating = false;
        } else {
            $ret = true;
        }
        
        return $ret;
    }
    
    /**
     * Authenticate using SASL Digest-MD5
     * @return bool
     */
    protected function saslAuthDigestMd5()
    {
        $this->binRequest([
            'opcode' => 0x21,
            'key' => 'DIGEST-MD5',
        ]);
        $data = $this->binResponse();
        if (0x21 == $this->getLastErrNo()) {
            $auth = new SaslAuthDigestMd5();
            $challengeResponse = $auth->getChallengeResponse(
                $this->username, $this->password, $data['value'], 'rozhenko', 'memcached'
            );
            $this->binRequest([
                'opcode' => 0x22,
                'key' => 'DIGEST-MD5',
                'value' => $challengeResponse
            ]);
            $data = $this->binResponse();
            if(0x21 == $this->getLastErrNo()) {
                $this->binRequest([
                    'opcode' => 0x22,
                    'key' => 'DIGEST-MD5'
                ]);
            }
            $data = $this->binResponse();
        }
        
        return Memcached::RES_SUCCESS == $this->getLastErrNo();
    }
    
    /**
     * Authenticate using SASL CRAM-MD5
     * @return bool
     */
    protected function saslAuthCramMd5()
    {
        $this->binRequest([
            'opcode' => 0x21,
            'key' => 'CRAM-MD5',
        ]);
        $data = $this->binResponse();
        if (0x21 == $this->getLastErrNo()) {
            $this->binRequest([
                'opcode' => 0x22,
                'key' => 'CRAM-MD5',
                'value' => $this->username
                    . ' '
                    . hash_hmac('md5', $data['value'], $this->password)
            ]);
            $data = $this->binResponse();
        }
        
        return Memcached::RES_SUCCESS == $this->getLastErrNo();
    }
    
    /**
     * Authenticate using SASL LOGIN
     * @return bool
     */
    protected function saslAuthLogin()
    {
        $this->binRequest([
            'opcode' => 0x21,
            'key' => 'LOGIN',
            'value' => $this->username
        ]);
        $data = $this->binResponse();
        if(0x21 == $this->getLastErrNo()) {
            $this->binRequest([
                'opcode' => 0x22,
                'key' => 'LOGIN',
                'value' => $this->password
            ]);
            $data = $this->binResponse();
        }

        return Memcached::RES_SUCCESS == $this->getLastErrNo();
    }
    
    /**
     * Authenticate using SASL PLAIN
     * @return bool
     */
    protected function saslAuthPlain()
    {
        $this->binRequest([
            'opcode' => 0x21,
            'key' => 'PLAIN',
            'value' => '' . chr(0) . $this->username . '@' . trim(`hostname`) . chr(0) . $this->password,
        ]);
        $data = $this->binResponse();
        
        return Memcached::RES_SUCCESS == $this->getLastErrNo();
    }
    
    /**
     * Authenticate using SASL ANONYMOUS
     * @return bool
     */
    protected function saslAuthAnonymous()
    {
        $this->binRequest([
            'opcode' => 0x21,
            'key' => 'ANONYMOUS',
            'value' => $this->username,
        ]);
        $data = $this->binResponse();
        
        return Memcached::RES_SUCCESS == $this->getLastErrNo();
    }
    
    
    /**
     * Return all keys currently held on servers
     * @return array|int
     */
    public function getAllKeysWithCacheDump()
    {
        if (true === $this->isBinaryProtocol()) {
            trigger_error(
                'function [' . __FUNCTION__ . '] not implemented in binary protocol',
                E_USER_WARNING
            );
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
        if (true === $this->isBinaryProtocol()) {
            trigger_error(
                'function [' . __FUNCTION__ . '] not implemented in binary protocol',
                E_USER_WARNING
            );
            return false;
        }
        $ret = [];
        $command = "lru_crawler metadump all\r\n";
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
     * Allows runtime adjustments of the cache memory limit
     * @param $size
     * @param bool $noreply
     * @return bool
     */
    public function cache_memlimit($size, $noreply = false)
    {
        if (true === $this->isBinaryProtocol()) {
            trigger_error(
                'function [' . __FUNCTION__ . '] not implemented in binary protocol',
                E_USER_WARNING
            );
            return false;
        }
        $ret = '';
        if (false === $noreply) {
            $noreply = '';
        } else {
            $noreply = 'noreply';
        }
        $size = min($size, $this->maxAllowableMem);
        $command = rtrim(preg_replace('/\s+/', ' ', "cache_memlimit {$size} {$noreply}"));
        $ret = $this->executeSimpleCommand($this->conn, $command);
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
        $ret = '';
        $level = max(min($level, 3), 0);
        if(null === $this->serverVersion) {
            $this->serverVersion = $this->version();
        }

        $match = [];
        preg_match('/^([0-9]+\.[0-9]+\.[0-9]+).*/', $this->serverVersion, $match);
        $versionMatch = version_compare($match[1], '1.6.0') >= 0;
        if (true === $this->isBinaryProtocol()) {
            if ($versionMatch) {
                $this->binRequest(['opcode' => 0x1b, 'extra' => pack('N', $level)]);
                $data = $this->binResponse();
                if ($this->lastErrNo == Memcached::RES_SUCCESS) {
                    
                    return true;
                } else {
                    
                    return false;
                }
            } else {
                trigger_error(
                    'function [' . __FUNCTION__ . '] not implemented in version < 1.6.0',
                    E_USER_WARNING
                );
                
                return false;
            }
        }

        if (false === $noreply) {
            $noreply = '';
        } else {
            $noreply = 'noreply';
        }
        $command = rtrim(preg_replace('/\s+/', ' ', "verbosity {$level} {$noreply}"));
        $ret = $this->executeSimpleCommand($this->conn, $command);
        return $ret == "OK\r\n";
    }

    /**
     * Send no-op command
     * @return bool
     */
    public function noop()
    {
        if (true === $this->isBinaryProtocol()) {
            $this->binRequest(['opcode' => 0x0a]);
            $data = $this->binResponse();
            if (Memcached::RES_SUCCESS == $this->lastErrNo) {
                $this->lastErrMsg = '';
                return true;
            } else {
                return false;
            }
        } else {
            trigger_error(
                'function [' . __FUNCTION__ . '] not implemented in ASCII protocol',
                E_USER_WARNING
            );
            return false;
        }
    }
    
    /**
     * Upon receiving this command, the server closes the connection
     * @return true|string
     */
    public function quit()
    {
        $ret = true;
        if (is_resource($this->conn)) {
            if (true === $this->isBinaryProtocol()) {
                $this->binRequest(['opcode' => 0x07]);
                $data = $this->binResponse();
                if (Memcached::RES_SUCCESS == $this->lastErrNo) {
                    $this->lastErrMsg = "disconnected from {$this->host}:{$this->port}.";
                }
            } else {
                $ret = $this->executeSimpleCommand($this->conn, 'quit');
                if (false !== $ret) {
                    $ret = true;
                }
            }
            fclose($this->conn);
            $this->conn = null;
        }
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
     * Handle memcached binary request
     * @param array $data 
     * @return bool | int
     */
    protected function binRequest(array $pipeItems)
    {
        if (false === $this->connect()) {
            
            return false;
        }
        
        $pipeline = '';
        $isPipeline = true;
        foreach (array_keys($pipeItems) as $k) {
            if(!is_int($k)) {
                $isPipeline = false;
                break;
            }
        }
        
        if (! $isPipeline) {
            $pipeItems = [$pipeItems];
        }
        
        foreach ($pipeItems as $data) {
            $request = '';
            $valuelength = $extralength = $keylength = 0;

            if (array_key_exists('extra', $data))
            {
                $extralength = strlen($data['extra']);
            }

            if (array_key_exists('key', $data))
            {
                if (! in_array($data['opcode'], [0x10, 0x21, 0x22])) {
                    if ('' !== $this->options[Memcached::OPT_PREFIX_KEY]) {
                        $data['key'] = $this->options[Memcached::OPT_PREFIX_KEY] . $data['key'];
                    }
                }
                $keylength = strlen($data['key']);
            }

            if (array_key_exists('value', $data))
            {
                $valuelength = strlen($data['value']);
            }

            $bodylength = $extralength + $keylength + $valuelength;

            $request = pack(
                'CCnCCnNNJ',
                0x80,
                $data['opcode'],
                $keylength,
                $extralength,
                array_key_exists('datatype', $data) ? $data['datatype'] : null,
                array_key_exists('vbucket_id', $data) ? $data['vbucket_id'] : null,
                $bodylength,
                array_key_exists('Opaque', $data) ? $data['Opaque'] : null,
                array_key_exists('CAS', $data) ? $data['CAS'] : null
            );

            if (array_key_exists('extra', $data))
            {
                $request .= $data['extra'];
            }

            if (array_key_exists('key', $data))
            {
                $request .= $data['key'];
            }

            if (array_key_exists('value', $data))
            {
                $request .= $data['value'];
            }
            
            $pipeline .= $request;
        }

        $sent = fwrite($this->conn, $pipeline);

        return $sent;
    }

    /**
     * Handle memcached binary response
     * @return bool | mixed
     */
    protected function binResponse()
    {
        if (false === $this->connect()) {

            return false;
        }
        
        $data = fread($this->conn, 24);

        if (false === $data || $data == '')
        {
            throw new SocketReadException("No Response from server {$this->host}:{$this->port}");
        }
        $response = unpack(
            'Cmagic/Copcode/nkeylength/Cextralength/Cdatatype/nstatus/Nbodylength/NOpaque/JCAS', 
            $data
        );
        $this->lastErrNo = $response['status'];
        $this->lastErrMsg = Memcached::RES_SUCCESS === $this->lastErrNo ? '' : 'undefined error';

        if (array_key_exists('bodylength', $response))
        {
            $bodylength = $response['bodylength'];
            $data = '';
            while ($bodylength > 0)
            {
                $binData = fread($this->conn, $bodylength);
                $bodylength -= strlen($binData);
                $data .= $binData;
            }

            if (array_key_exists('extralength', $response) && $response['extralength'] > 0)
            {
                $extra = unpack('Nint', substr($data, 0, $response['extralength']));
                $response['extra'] = $extra['int'];
            }

            $response['key'] = substr($data, $response['extralength'], $response['keylength']);
            if (! in_array($response['opcode'], [0x10, 0x21, 0x22])) {
                if ('' !== $this->options[Memcached::OPT_PREFIX_KEY]) {
                    $response['key'] = substr($response['key'], strlen($this->options[Memcached::OPT_PREFIX_KEY]));
                }
            }
            
            $response['body'] = substr($data, $response['extralength'] + $response['keylength']);
            
            if(Memcached::RES_SUCCESS !== $this->lastErrNo) {
                $this->lastErrMsg = $response['body'];
            }
        }
        if (!array_key_exists('extra', $response))
        {
            $response['extra'] = 0;
        }
        $ret = [
            'key' => $response['key'],
            'value' => $this->setType($response['body'], $response['extra']),
            'cas' => $response['CAS'],
            'flags' => $response['extra'],
        ];
        
        return $ret;
    }

    /**
     * Check wether binary protocol is on and authentication is required
     * @return bool
     */
    public function isBinaryProtocol()
    {
        return true === $this->options[Memcached::OPT_BINARY_PROTOCOL];
    }

    /**
     * Check wether binary protocol is on and authentication is required
     * @return bool
     */
    protected function needAuth()
    {
        return true === $this->isBinaryProtocol()
                && Memcached::SASL_AUTH_NONE != $this->options[Memcached::OPT_SASL_AUTH_METHOD];
    }
    
    protected function hasCredentials()
    {
        return '' != $this->username . $this->password;
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
        if(true === $this->options[Memcached::OPT_COMPRESSION]) {
            $condLenString = 1 == $v && $this->options[Memcached::OPT_COMPRESSION_THRESHOLD] < strlen($value);
            $v += (($v & 128) == 128) || $condLenString? 256 : 0; // long string, array or object can be compressed
        }
        return $v;
    }
    
    /**
     * Prepare data for storage, serialization and compression may happen
     * @param $value
     * @return array[int, mixed]
     */
    protected function prepareStoredData($value)
    {
        $flag = $this->flag($value);
        if ($flag & 128) {
            $value = $this->serializer->serialize($value);
        }
        if (true === $this->options[Memcached::OPT_COMPRESSION] && null !== $this->compressor && ($flag & 256)) {
            $tmp = $this->compressor->compress($value);
            if(strlen($value) > strlen($tmp) * $this->options[Memcached::OPT_COMPRESSION_FACTOR]) {
                $value = $tmp;
            } else {
                $flag -= 256;
            }
            unset($tmp);
        }
        
        return [$flag, $value];
    }

    /**
     * Restore original type from stored value
     * @param $value
     * @param $flag
     * @return mixed
     */
    protected function setType($value, $flag)
    {
        if (0 < $flag) { 
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
        }
        
        return $value;
    }
}
