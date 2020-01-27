<?php

namespace Djiele\Memcached;

use Flexihash\Flexihash;
use Flexihash\Exception;
use Djiele\Memcached\DistributedHash\DistributedHashModula;
use Flexihash\Hasher\HasherInterface;
use Flexihash\Hasher\Md5Hasher;
use Flexihash\Hasher\Crc32Hasher;
use Djiele\Memcached\Hasher\Fnv164Hasher;
use Djiele\Memcached\Hasher\Fnv1A64Hasher;
use Djiele\Memcached\Hasher\Fnv132Hasher;
use Djiele\Memcached\Hasher\Fnv1A32Hasher;
use Djiele\Memcached\Hasher\HsiehHasher;
use Djiele\Memcached\Hasher\MurmurHasher;
use Djiele\Memcached\Hasher\OneAtAtimeHasher;

class Memcached
{
    const COMPRESSION_DEFAULT = 0;
    const COMPRESSION_FASTLZ = 1;
    const COMPRESSION_GZ = 2;
    const SERIALIZER_PHP = 1;
    const SERIALIZER_IGBINARY = 2;
    const SERIALIZER_JSON = 3;
    const SERIALIZER_MSGPACK = 4;
    const HASH_DEFAULT = 0;
    const HASH_MD5 = 1;
    const HASH_CRC = 2;
    const HASH_FNV1_64 = 3;
    const HASH_FNV1A_64 = 4;
    const HASH_FNV1_32 = 5;
    const HASH_FNV1A_32 = 6;
    const HASH_HSIEH = 7;
    const HASH_MURMUR = 8;
    const DISTRIBUTION_MODULA = 0;
    const DISTRIBUTION_CONSISTENT = 1;
    const HAVE_IGBINARY = 1;
    const HAVE_JSON = 1;
    const SASL_AUTH_NONE = 1;
    const SASL_AUTH_DIGEST_MD5 = 2;
    const SASL_AUTH_CRAM_MD5 = 3;
    const SASL_AUTH_LOGIN = 4;
    const SASL_AUTH_PLAIN = 5;
    const SASL_AUTH_ANONYMOUS = 6;
    //const SASL_AUTH_NTLM = 7;
    const GET_PRESERVE_ORDER = 1;
    const GET_EXTENDED = 2;

    const OPT_DISTRIBUTION = 9;     // MEMCACHED_BEHAVIOR_DISTRIBUTION
    const OPT_HASH = 2;     //MEMCACHED_BEHAVIOR_HASH
    const OPT_COMPRESSION = -1001;
    const OPT_COMPRESSION_TYPE = 24;
    const OPT_COMPRESSION_FACTOR = 22;
    const OPT_COMPRESSION_THRESHOLD = 23;
    const OPT_PREFIX_KEY = -1002;
    const OPT_SERIALIZER = -1003;
    const OPT_LIBKETAMA_COMPATIBLE = 16;    // MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED
    const OPT_BUFFER_WRITES = 10;           // MEMCACHED_BEHAVIOR_BUFFER_REQUESTS
    const OPT_BINARY_PROTOCOL = 18;         // MEMCACHED_BEHAVIOR_BINARY_PROTOCOL
    const OPT_NO_BLOCK = 0;                 // MEMCACHED_BEHAVIOR_NO_BLOCK
    const OPT_TCP_NODELAY = 1;              // MEMCACHED_BEHAVIOR_TCP_NODELAY
    const OPT_SOCKET_SEND_SIZE = 4;         // MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE
    const OPT_SOCKET_RECV_SIZE = 5;         // MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE
    const OPT_CONNECT_TIMEOUT = 14;         // MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT
    const OPT_RETRY_TIMEOUT = 15;           // MEMCACHED_BEHAVIOR_RETRY_TIMEOUT
    const OPT_SEND_TIMEOUT = 19;            // MEMCACHED_BEHAVIOR_SND_TIMEOUT
    const OPT_RECV_TIMEOUT = 20;            // MEMCACHED_BEHAVIOR_RCV_TIMEOUT
    const OPT_POLL_TIMEOUT = 8;             // MEMCACHED_BEHAVIOR_POLL_TIMEOUT
    const OPT_CACHE_LOOKUPS = 6;            // MEMCACHED_BEHAVIOR_CACHE_LOOKUPS
    const OPT_SERVER_FAILURE_LIMIT = 21;    // MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT
    const OPT_SASL_AUTH_METHOD = 25;


    const RES_SUCCESS = 0;                  // MEMCACHED_SUCCESS
    const RES_FAILURE = 1;                  // MEMCACHED_FAILURE
    const RES_HOST_LOOKUP_FAILURE = 2;      // MEMCACHED_HOST_LOOKUP_FAILURE
    const RES_UNKNOWN_READ_FAILURE = 7;     // MEMCACHED_UNKNOWN_READ_FAILURE
    const RES_PROTOCOL_ERROR = 8;           // MEMCACHED_PROTOCOL_ERROR
    const RES_CLIENT_ERROR = 9;             // MEMCACHED_CLIENT_ERROR
    const RES_SERVER_ERROR = 10;            // MEMCACHED_SERVER_ERROR
    const RES_WRITE_FAILURE = 5;            // MEMCACHED_WRITE_FAILURE
    const RES_DATA_EXISTS = 12;             // MEMCACHED_DATA_EXISTS
    const RES_NOTSTORED = 14;               // MEMCACHED_NOTSTORED
    const RES_NOTFOUND = 16;                // MEMCACHED_NOTFOUND
    const RES_PARTIAL_READ = 18;            // MEMCACHED_PARTIAL_READ
    const RES_SOME_ERRORS = 19;             // MEMCACHED_SOME_ERRORS
    const RES_NO_SERVERS = 20;              // MEMCACHED_NO_SERVERS
    const RES_END = 21;                     // MEMCACHED_END
    const RES_ERRNO = 26;                   // MEMCACHED_ERRNO
    const RES_BUFFERED = 32;                // MEMCACHED_BUFFERED
    const RES_TIMEOUT = 31;                 // MEMCACHED_TIMEOUT
    const RES_BAD_KEY_PROVIDED = 33;        // MEMCACHED_BAD_KEY_PROVIDED
    const RES_CONNECTION_SOCKET_CREATE_FAILURE = 11;    // MEMCACHED_CONNECTION_SOCKET_CREATE_FAILURE
    const RES_PAYLOAD_FAILURE = -1001;

    protected $option = array(
        self::OPT_COMPRESSION => true,
        self::OPT_COMPRESSION_FACTOR => 1.3,
        self::OPT_COMPRESSION_THRESHOLD => 2000,
        self::OPT_COMPRESSION_TYPE => self::COMPRESSION_FASTLZ,
        self::OPT_SERIALIZER => self::SERIALIZER_IGBINARY,
        self::OPT_PREFIX_KEY => '',
        self::OPT_HASH => self::HASH_DEFAULT,
        self::OPT_DISTRIBUTION => self::DISTRIBUTION_CONSISTENT,
        self::OPT_LIBKETAMA_COMPATIBLE => false,
        self::OPT_BUFFER_WRITES => false,
        self::OPT_BINARY_PROTOCOL => false,
        self::OPT_NO_BLOCK => false,
        self::OPT_TCP_NODELAY => false,
        self::OPT_SOCKET_SEND_SIZE => 32767,
        self::OPT_SOCKET_RECV_SIZE => 65535,
        self::OPT_CONNECT_TIMEOUT => 10,
        self::OPT_RETRY_TIMEOUT => 0,
        self::OPT_SEND_TIMEOUT => 0,
        self::OPT_RECV_TIMEOUT => 0,
        self::OPT_POLL_TIMEOUT => 1000,
        self::OPT_CACHE_LOOKUPS => false,
        self::OPT_SERVER_FAILURE_LIMIT => 0,
        self::OPT_SASL_AUTH_METHOD => self::SASL_AUTH_NONE,
    );
    protected static $instances = [];
    protected $persistentId = null;
    protected $hasher = null;
    protected $distributedHash = null;
    protected $username = null;
    protected $password = null;
    protected $resultCode = 0;
    protected $resultMessage = '';
    protected $server = [];
    protected $delayedResult = [];

    /**
     * Memcached constructor.
     * @param null $persistent_id
     */
    public function __construct($persistent_id = null)
    {
        $this->persistentId = $persistent_id;
        $this->setHasher();
        $this->setDistribution();
        self::$instances[] = &$this;
    }

    /**
     * set Sasl AuthData
     * @param $username
     * @param $password
     */
    public function setSaslAuthData($username, $password)
    {
        $this->setCredentials($username, $password);
        if (true === $this->option[self::OPT_BINARY_PROTOCOL]) {
            foreach ($this->server as $serverKey => &$serverData) {
                $serverData['client']->sasl_auth($username, $password);
            }
        } else {
            trigger_error(
                'Memcached::setSaslAuthData(): SASL is only supported with binary protocol',
                E_USER_WARNING
            );
        }
    }

    /**
     * Checks if the connections to the memcache servers are persistent connections.
     * @return bool
     */
    public function isPersistent()
    {
        return false;
    }

    /**
     * Check if the instance was recently created
     * @return bool
     */
    public function isPristine()
    {
        return (0 < count(self::$instances)) && (self::$instances[0] === $this);
    }

    /**
     *  Add a server to the server pool
     * @param $host
     * @param int $port
     * @param int $weight
     * @param bool $init
     * @return bool
     * @throws Exception
     */
    public function addServer($host, $port = 11211, $weight = 1, $init = true)
    {
        $weight = max(1, $weight);
        $candidate = new MemcachedClient($host, $port, $this->option);
        $key = "{$host}:{$port}:{$weight}";
        $this->server[$key] = [
            'host' => $host,
            'port' => intval($port),
            'weight' => intval($weight),
            'client' => & $candidate,
        ];
        if (true === $init) {
            $this->distributedHash->addTarget($key, $weight);
        } else {
            $this->relocateKeys($key, $weight, true);
        }
        $this->resultCode = self::RES_SUCCESS;
        $this->resultMessage = '';
        
        return true;
    }

    /**
     * Add multiple servers to the server pool
     *
     * @param array $servers
     * @param bool $init
     * @return  boolean
     * @throws Exception
     */
    public function addServers($servers, $init = true)
    {
        $count = 0;
        foreach ($servers as $svr) {
            $host = array_shift($svr);
            $port = array_shift($svr);
            if (is_null($port)) {
                $port = 11211;
            }
            $weight = array_shift($svr);
            if (is_null($weight)) {
                $weight = 0;
            }
            if (true === $this->addServer($host, $port, $weight, $init)) {
                ++$count;
            } else {
                trigger_error("{$host}:{$port}:{$weight} could not be added", E_USER_NOTICE);
            }
        }
        if ($count == count($servers)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'One or more server could not be added';
        }
        return (0 < $count);
    }

    /**
     * Attempt to relocate key/value pairs in case of new server or server going offline
     * @param $serverKey
     * @param $serverWeight
     * @param bool $added
     * @throws Exception
     */
    protected function relocateKeys($serverKey, $serverWeight, $added = true)
    {
        $movedKeys = [];
        $movedByBytes = 0;
        $keysBefore = array_flip($this->getAllKeys());
        $keysAfter = [];
        foreach (array_keys($keysBefore) as $key) {
            $keysBefore[$key] = $this->distributedHash->lookup($key);
        }
        if (true === $added) {
            $this->distributedHash->addTarget($serverKey, $serverWeight);
        } else {
            $this->distributedHash->removeTarget($serverKey);
        }
        foreach (array_keys($keysBefore) as $key) {
            $keysAfter[$key] = $this->distributedHash->lookup($key);
        }
        foreach ($keysAfter as $key => $serverKey) {
            if ($keysBefore[$key] != $keysAfter[$key]) {
                $value = $this->getByKey($keysBefore[$key], $key, null, 0, true);
                $movedKeys[] = $key;
                $movedByBytes += strlen($value);
                if($this->addByKey($serverKey, $key, $value, 30)) {
                    $this->deleteByKey($keysBefore[$key], $key, 0);
                }
            }
        }
    }

    /**
     * Clears all servers from the server list
     * @return bool
     */
    public function resetServerList()
    {
        $this->quit();
        foreach (array_keys($this->getServerList()) as $svrKey) {
            $this->server[$svrKey]['client'] = null;
        }
        $this->server = [];
        $this->resultCode = self::RES_SUCCESS;
        $this->resultMessage = '';
        return true;
    }

    /**
     * Close any open connections
     * @return bool
     */
    public function quit()
    {
        foreach ($this->getServerList() as $svrKey => $svr) {
            $this->server[$svrKey]['client']->quit();
        }
        $this->resultCode = self::RES_SUCCESS;
        $this->resultMessage = '';
        return true;
    }

    /**
     * Get the list of the servers in the pool
     * @return array
     */
    public function getServerList()
    {
        if(0 == count($this->server)) {
            $this->resultCode = self::RES_NO_SERVERS;
            $this->resultMessage = 'No server in pool';
        } else {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
        }
        return $this->server;
    }

    /**
     * Map a key to a server
     * @param $skey
     * @return string
     */
    public function getServerByKey($skey)
    {
        $this->resultCode = self::RES_SUCCESS;
        $this->resultMessage = '';
        return $this->distributedHash->lookup($skey);
    }

    /**
     * Set a Memcached option
     * @param $option
     * @param $value
     * @return bool
     */
    public function setOption($option, $value)
    {
        $this->option[$option] = $value;
        if (self::OPT_HASH == $option) {
            $this->setHasher();
        } elseif(self::OPT_DISTRIBUTION == $option) {
            $this->setDistribution($this->option[self::OPT_DISTRIBUTION]);
        }
        foreach ($this->getServerList() as $svrKey => $svr) {
            $this->server[$svrKey]['client']->setOption($option, $value);
        }
        $this->resultCode = self::RES_SUCCESS;
        $this->resultMessage = '';

        return true;
    }

    /**
     * Set Memcached options
     * @param $options
     * @return bool
     */
    public function setOptions($options)
    {
        foreach ($options as $key => $value) {
            $this->setOption($key, $value);
        }

        $this->resultCode = self::RES_SUCCESS;
        $this->resultMessage = '';
        return true;
    }

    /**
     * Retrieve a Memcached option value
     * @param $option
     * @return bool|mixed
     */
    public function getOption($option)
    {
        if (isset($this->option[$option])) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $this->option[$option];

        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'Option not set.';
            return false;
        }
    }

    /**
     * Get server pool statistics
     * @return array
     */
    public function getStats()
    {
        $ret = [];
        foreach ($this->getServerList() as $k => $v) {
            $ret[$v['host'] . ':' . $v['port']] = $v['client']->stats();
        }
        return $ret;
    }

    /**
     * Get server pool version info
     * @return array
     */
    public function getVersion()
    {
        $ret = [];
        foreach ($this->getServerList() as $k => $v) {
            if (false === ($version = $v['client']->version())) {
                $version = '255.255.255';
            }
            $ret[$v['host'] . ':' . $v['port']] = $version;
        }
        return $ret;
    }

    /**
     * Return the result code of the last operation
     * @return int
     */
    public function getResultCode()
    {
        return $this->resultCode;
    }

    /**
     * Return the message describing the result of the last operation
     * @return string
     */
    public function getResultMessage()
    {
        return $this->resultMessage;
    }

    /**
     * Delete an item
     * @param $key
     * @param int $time
     * @return bool
     * @throws Exception
     */
    public function delete($key, $time = 0)
    {
        $host = $this->distributedHash->lookup($key);
        if ($this->server[$host]['client']->delete($key)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_NOTFOUND;
            $this->resultMessage = 'Delete fail, key not exists.';
            return false;
        }
    }

    /**
     * Delete an item from a specific server
     * @param $skey
     * @param $key
     * @param int $time
     * @return bool
     * @throws Exception
     */
    public function deleteByKey($skey, $key, $time = 0)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->delete($key)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_NOTFOUND;
            $this->resultMessage = 'Delete fail, key not exists.';
            return false;
        }
    }

    /**
     * Delete multiple items
     * @param array $keys
     * @param int $time
     * @return array
     * @throws Exception
     */
    public function deleteMulti(array $keys, $time = 0)
    {
        $ret = [];
        foreach ($keys as $key) {
            if ($this->delete($key, $time)) {
                $ret[$key] = self::RES_SUCCESS;
            } else {
                $ret[$key] = self::RES_NOTFOUND;
            }
        }
        return $ret;
    }

    /**
     * Delete multiple items from a specific server
     * @param $skey
     * @param array $keys
     * @param int $time
     * @return array
     * @throws Exception
     */
    public function deleteMultiByKey($skey, array $keys, $time = 0)
    {
        $ret = [];
        foreach ($keys as $key) {
            if ($this->deleteByKey($skey, $key, $time)) {
                $ret[$key] = self::RES_SUCCESS;
            } else {
                $ret[$key] = self::RES_NOTFOUND;
            }
        }
        return $ret;
    }

    /**
     * Invalidate all items in the cache
     * @param int $delay
     * @return bool
     */
    public function flush($delay = 0)
    {
        $i = 0;
        foreach ($this->getServerList() as $svrKey => $svr) {
            $this->server[$svrKey]['client']->flush_all($delay + (2 * $i++));
        }
        return true;
    }

    /**
     * Add an item under a new key
     * @param $key
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function add($key, $val, $expt = 0)
    {
        $host = $this->distributedHash->lookup($key);
        if ($this->server[$host]['client']->add($key, $val, $expt)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'The key [' . $key . '] already exists.';
            return false;
        }
    }

    /**
     * Add an item under a new key on a specific server
     * @param $skey
     * @param $key
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function addByKey($skey, $key, $val, $expt = 0)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->add($key, $val, $expt)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'The key [' . $key . '] already exists.';
            return false;
        }
    }

    /**
     * Store an item
     * @param $key
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function set($key, $val, $expt = 0)
    {
        $host = $this->distributedHash->lookup($key);
        if ($this->server[$host]['client']->set($key, $val, $expt)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'Set fail.';
            return false;
        }
    }

    /**
     * Store an item on a specific server
     * @param $skey
     * @param $key
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function setByKey($skey, $key, $val, $expt = 0)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->set($key, $val, $expt)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'Set fail.';
            return false;
        }
    }

    /**
     * Store multiple items
     * @param $keys
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function setMulti($keys, $expt = 0)
    {
        $ret = true;
        foreach ($keys as $key => $value) {
            if (!$this->set($key, $value, $expt)) {
                $this->resultCode = self::RES_FAILURE;
                $this->resultMessage = 'One or more keys could not be stored';
                $ret = false;
            }
        }
        if (true === $ret) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
        }
        return $ret;
    }

    /**
     * Store multiple items on a specific server
     * @param $skey
     * @param $keys
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function setMultiByKey($skey, $keys, $val, $expt = 0)
    {
        $ret = true;
        foreach ($keys as $key => $value) {
            if (!$this->setByKey($skey, $key, $value, $expt)) {
                $this->resultCode = self::RES_FAILURE;
                $this->resultMessage = 'One or more keys could not be stored';
                $ret = false;
            }
        }
        if (true === $ret) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
        }
        return $ret;
    }

    /**
     * Compare and swap an item
     * @param $cas_token
     * @param $key
     * @param $value
     * @param $expiration
     * @return bool
     * @throws Exception
     */
    public function cas($cas_token, $key, $value, $expiration)
    {
        $host = $this->getServerByKey($key);
        if ($this->server[$host]['client']->cas($key, $value, $expiration, $cas_token)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'cas key {$key} failed';
            return false;
        }
    }

    /**
     * Compare and swap an item on a specific server
     * @param $cas_token
     * @param $skey
     * @param $key
     * @param $value
     * @param $expiration
     * @return bool
     * @throws Exception
     */
    public function casByKey($cas_token, $skey, $key, $value, $expiration)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->cas($key, $value, $expiration, $cas_token)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'cas key {$key} failed';
            return false;
        }
    }

    /**
     * Prepend data to an existing item
     * @param $key
     * @param $value
     * @return bool
     * @throws Exception
     */
    public function prepend($key, $value)
    {
        $host = $this->getServerByKey($key);
        if ($this->server[$host]['client']->prepend($key, $value)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = $this->server[$host]['client']->getLastErrMsg();
            return false;
        }
    }

    /**
     * Prepend data to an existing item on a specific server
     * @param $skey
     * @param $key
     * @param $value
     * @return bool
     * @throws Exception
     */
    public function prependByKey($skey, $key, $value)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->prepend($key, $value)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = $this->server[$host]['client']->getLastErrMsg();
            return false;
        }
    }

    /**
     *  Append data to an existing item
     * @param $key
     * @param $value
     * @return bool
     * @throws Exception
     */
    public function append($key, $value)
    {
        $host = $this->getServerByKey($key);
        if ($this->server[$host]['client']->append($key, $value)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = $this->server[$host]['client']->getLastErrMsg();
            return false;
        }
    }

    /**
     * Append data to an existing item on a specific server
     * @param $skey
     * @param $key
     * @param $value
     * @return bool
     * @throws Exception
     */
    public function appendByKey($skey, $key, $value)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->append($key, $value)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = $this->server[$host]['client']->getLastErrMsg();
            return false;
        }
    }

    /**
     * Replace the item under an existing key
     * @param $key
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function replace($key, $val, $expt = 0)
    {
        $host = $this->distributedHash->lookup($key);
        if ($this->server[$host]['client']->replace($key, $val, $expt)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'The key [' . $key . '] not found.';
            return false;
        }
    }

    /**
     * Replace the item under an existing key on a specific server
     * @param $skey
     * @param $key
     * @param $val
     * @param int $expt
     * @return bool
     * @throws Exception
     */
    public function replaceByKey($skey, $key, $val, $expt = 0)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->replace($key, $val, $expt)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'The key [' . $key . '] not found.';
            return false;
        }
    }

    /**
     * Increment numeric item's value
     * @param $key
     * @param int $offset
     * @param int $initial_value
     * @param int $expiry
     * @return mixed int|bool
     * @throws Exception
     */
    public function increment($key, $offset = 1, $initial_value = 0, $expiry = 0)
    {
        $host = $this->distributedHash->lookup($key);
        $value = $this->server[$host]['client']->incr($key, $offset, $initial_value, $expiry);
        if (0 == $this->server[$host]['client']->getLastErrNo()) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $value;
        } else {
            $this->add($key, $initial_value, $expiry);
            if(0 == $this->server[$host]['client']->getLastErrNo()) {
                return $initial_value;
            } else {
                return false;
            }
        }
    }

    /**
     * Increment numeric item's value, stored on a specific server
     * @param $skey
     * @param $key
     * @param int $offset
     * @param int $initial_value
     * @param int $expiry
     * @return bool
     * @throws Exception
     */
    public function incrementByKey($skey, $key, $offset = 1, $initial_value = 0, $expiry = 0)
    {
        $host = $this->getServerByKey($skey);
        $value = $this->server[$host]['client']->incr($key, $offset, $initial_value, $expiry);
        if (0 == $this->server[$host]['client']->getLastErrNo()) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $value;
        } else {
            return $this->add($key, $initial_value, $expiry);
        }
    }

    /**
     * Decrement numeric item's value
     * @param $key
     * @param int $offset
     * @param int $initial_value
     * @param int $expiry
     * @return bool
     * @throws Exception
     */
    public function decrement($key, $offset = 1, $initial_value = 0, $expiry = 0)
    {
        $host = $this->distributedHash->lookup($key);
        $value = $this->server[$host]['client']->decr($key, $offset, $initial_value, $expiry);
        if (0 == $this->server[$host]['client']->getLastErrNo()) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $value;
        } else {
            $this->add($key, $initial_value, $expiry);
            if (0 == $this->server[$host]['client']->getLastErrNo()) {
                return $initial_value;
            } else {
                return false;
            }
        }
    }

    /**
     * Decrement numeric item's value, stored on a specific server
     * @param $skey
     * @param $key
     * @param int $offset
     * @param int $initial_value
     * @param int $expiry
     * @return bool
     * @throws Exception
     */
    public function decrementByKey($skey, $key, $offset = 1, $initial_value = 0, $expiry = 0)
    {
        $host = $this->getServerByKey($skey);
        $value = $this->server[$host]['client']->decr($key, $offset, $initial_value, $expiry);
        if (0 == $this->server[$host]['client']->getLastErrNo()) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $value;
        } else {
            return $this->add($key, $initial_value, $expiry);
        }
    }

    /**
     * Set a new expiration on an item
     * @param $key
     * @param $expiration
     * @return bool
     * @throws Exception
     */
    public function touch($key, $expiration)
    {
        $host = $this->getServerByKey($key);
        if ($this->server[$host]['client']->touch($key, $expiration)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'The key [' . $key . '] could not be touched.';
            return false;
        }
    }

    /**
     * Set a new expiration on an item on a specific server
     * @param $skey
     * @param $key
     * @param $expiration
     * @return bool
     * @throws Exception
     */
    public function touchByKey($skey, $key, $expiration)
    {
        $host = $this->getServerByKey($skey);
        if ($this->server[$host]['client']->touch($key, $expiration)) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return true;
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = 'The key [' . $key . '] could not be touched.';
            return false;
        }
    }

    /**
     * Retrieve an item
     * @param $key
     * @param null $cache_cb
     * @param int $flags
     * @return bool|mixed
     * @throws Exception
     */
    public function get($key, $cache_cb = null, $flags = 0)
    {
        $host = $this->distributedHash->lookup($key);
        return $this->getCommon($host, $key, $cache_cb, $flags);
    }

    /**
     * Retrieve an item from a specific server
     * @param $skey
     * @param $key
     * @param null $cache_cb
     * @param int $flags
     * @return bool|mixed
     * @throws Exception
     */
    public function getByKey($skey, $key, $cache_cb = null, $flags = 0)
    {
        $host = $this->getServerByKey($skey);
        return $this->getCommon($host, $key, $cache_cb, $flags);
    }

    /**
     * Common code for get and getByKey functions
     * @param $host
     * @param $key
     * @param null $cache_cb
     * @param int $flags
     * @return array|bool|mixed|null
     * @throws Exception
     */
    protected function getCommon($host, $key, $cache_cb = null, $flags = 0) {
        $value = $this->server[$host]['client']->gets([$key]);
        if (0 == $this->server[$host]['client']->getLastErrNo()) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $this->returnSingleValue($value, $flags);
        } else {
            if (is_callable($cache_cb)) {
                $newVal = null;
                if(true === $cache_cb($this, $key, $newVal)) {
                    if($this->server[$host]['client']->set($key, $newVal, 0)) {
                        if ($flags && self::GET_EXTENDED) {
                            $value = $this->server[$host]['client']->gets([$key]);
                            if (0 == $this->server[$host]['client']->getLastErrNo()) {
                                $this->resultCode = self::RES_SUCCESS;
                                $this->resultMessage = '';
                                return $this->returnSingleValue($value, $flags);
                            }
                        } else {
                            return $newVal;
                        }
                    }
                }
            }
        }
        $this->resultCode = self::RES_NOTFOUND;
        $this->resultMessage = 'Key [' . $key . '] not found.';
        return false;
    }

    /**
     * Retrieve multiple items
     * @param array $keys
     * @param int $flags
     * @return array
     * @throws Exception
     */
    public function getMulti(array $keys, $flags = 0)
    {
        $ret = array_combine(
            $keys,
            array_fill(0, count($keys), null)
        );
        $hosts = [];
        foreach ($keys as $key) {
            $host = $this->distributedHash->lookup($key);
            if (!array_key_exists($host, $hosts)) {
                $hosts[$host] = [$key];
            } else {
                $hosts[$host][] = $key;
            }
        }

        foreach ($hosts as $host => $keys) {
            $values = $this->server[$host]['client']->gets($keys);
            if (0 == $this->server[$host]['client']->getLastErrNo()) {
                $ret = $this->returnMultipleValues($values, $flags);
            } else {
                foreach ($hosts[$host] as $key) {
                    unset($ret[$key]);
                }
            }
        }
        return $ret;
    }

    /**
     * Retrieve multiple items from a specific server
     * @param $skey
     * @param array $keys
     * @param int $flags
     * @return array|false
     * @throws Exception
     */
    public function getMultiByKey($skey, array $keys, $flags = 0)
    {
        $host = $this->distributedHash->lookup($skey);
        $values = $this->server[$host]['client']->gets($keys);
        if (0 == $this->server[$host]['client']->getLastErrNo()) {
            $this->resultCode = self::RES_SUCCESS;
            $this->resultMessage = '';
            return $this->returnMultipleValues($values, $flags);
        } else {
            $this->resultCode = self::RES_FAILURE;
            $this->resultMessage = $this->server[$host]['client']->getLastErrMsg();
            return false;
        }
    }

    /**
     * Request multiple items
     * @param array $keys
     * @param bool $with_cas
     * @param null $value_cb
     * @return bool
     * @throws Exception
     */
    public function getDelayed(array $keys, $with_cas = false, $value_cb = null)
    {
        $this->delayedResult = [];
        if (true === $with_cas) {
            $flags = Memcached::GET_EXTENDED;
        } else {
            $flags = 0;
        }
        if (is_array($resultSet = $this->getMulti($keys, $flags))) {
            if(is_callable($value_cb)) {
                foreach($resultSet as $keyResult => $valueResult) {
                    $value_cb($this, array_merge(['key' => $keyResult], $valueResult));
                }
            } else {
                foreach($resultSet as $keyResult => $valueResult) {
                    $this->delayedResult[] = array_merge(['key' => $keyResult], $valueResult);
                }
            }
        }
        return true;
    }

    /**
     * Request multiple items from a specific server
     * @param $skey
     * @param array $keys
     * @param bool $with_cas
     * @param null $value_cb
     * @return bool
     * @throws Exception
     */
    public function getDelayedByKey($skey, array $keys, $with_cas = false, $value_cb = null)
    {
        $this->delayedResult = [];
        if (true === $with_cas) {
            $flags = Memcached::GET_EXTENDED;
        } else {
            $flags = 0;
        }
        if (is_array($resultSet = $this->getMultiByKey($skey, $keys, $flags))) {
            if(is_callable($value_cb)) {
                foreach($resultSet as $keyResult => $valueResult) {
                    $value_cb($this, array_merge(['key' => $keyResult], $valueResult));
                }
            } else {
                foreach($resultSet as $keyResult => $valueResult) {
                    $this->delayedResult[] = array_merge(['key' => $keyResult], $valueResult);
                }
            }
        }
        return true;
    }

    /**
     * Fetch the next result
     * @return bool|mixed
     */
    public function fetch()
    {
        if (0 == count($this->delayedResult)) {
            $this->resultCode = Memcached::RES_END;
            $this->resultMessage = 'No more key to fetch';
            return false;
        }
        return array_shift($this->delayedResult);
    }

    /**
     * Fetch all the remaining results
     * @return array
     */
    public function fetchAll()
    {
        $ret = $this->delayedResult;
        $this->delayedResult = [];
        return $ret;
    }

    /**
     * Gets the keys stored on all the servers
     * @return array
     */
    public function getAllKeys()
    {
        $keys = [];
        foreach ($this->getServerList() as $svr) {
            if (is_array($svrKeys = $svr['client']->getAllKeysWithCacheDump())) {
                $keys = array_unique(array_merge($keys, $svrKeys));
            }
            if (is_array($svrKeys = $svr['client']->getAllKeysWithLruCrawler())) {
                $keys = array_unique(array_merge($keys, $svrKeys));
            }
        }
        if(0 < ($lenPrefix =  strlen($this->option[self::OPT_PREFIX_KEY]))) {
            foreach($keys as $i => $k) {
                if(
                    0 === strpos($k, $this->option[self::OPT_PREFIX_KEY]) &&
                    $this->option[self::OPT_PREFIX_KEY] == substr($k, 0, $lenPrefix)
                ) {
                    $keys[$i] = substr($k, $lenPrefix);
                }
            }
        }

        return $keys;
    }

    /**
     * Instantiate Hasher object
     */
    protected function setHasher()
    {
        switch ($this->option[self::OPT_HASH]) {
            case self::HASH_MD5:
                $this->hasher = new Md5Hasher();
                break;
            case self::HASH_CRC:
                $this->hasher = new Crc32Hasher();
                break;
            case self::HASH_FNV1_64:
                $this->hasher = new Fnv164Hasher();
                break;
            case self::HASH_FNV1A_64:
                $this->hasher = new Fnv1A64Hasher();
                break;
            case self::HASH_FNV1_32:
                $this->hasher = new Fnv132Hasher();
                break;
            case self::HASH_FNV1A_32:
                $this->hasher = new Fnv1A32Hasher();
                break;
            case self::HASH_HSIEH:
                $this->hasher = new HsiehHasher();
                break;
            case self::HASH_MURMUR:
                $this->hasher = new MurmurHasher();
                break;
            default:
                $this->hasher = new OneAtAtimeHasher();
        }
    }

    /**
     * Instantiate distributed hash object
     */
    protected function setDistribution()
    {
        switch ($this->option[self::OPT_DISTRIBUTION]) {
            case self::DISTRIBUTION_CONSISTENT:
                $this->distributedHash = new Flexihash($this->hasher);
                break;
            default:
                $this->distributedHash = new DistributedHashModula($this->hasher);
        }
    }

    /**
     * Parse response array and return an appropriate single value
     * @param array $values
     * @param $flags
     * @return array|mixed
     */
    protected function returnSingleValue(array $values, $flags)
    {
        $values = current($values);
        if (self::GET_EXTENDED == ($flags & self::GET_EXTENDED)) {
            unset($values['key']);
        } else {
            $values = $values['value'];
        }

        return $values;
    }

    /**
     * Parse response array and return appropriate multiple values set
     * @param array $values
     * @param int $flags
     * @return array
     */
    protected function returnMultipleValues(array $values, $flags)
    {
        $ret = [];
        if(self::GET_EXTENDED == ($flags & self::GET_EXTENDED)) {
            foreach ($values as &$value) {
                $key = $value['key'];
                unset($value['key']);
                $ret[$key] = $value;
            }
        } else {
            foreach ($values as $value) {
                $ret[$value['key']] = $value['value'];
            }
        }

        return $ret;
    }
    
    /**
     * Initialize username and password class members
     * @param string $username
     * @param string $password
     */
    protected function setCredentials($username, $password)
    {
        $this->username = $username;
        $this->password = $password;
    }
}
