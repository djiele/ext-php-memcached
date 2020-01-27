<?php

namespace Djiele\Memcached\Hasher;

use Flexihash\Hasher\HasherInterface;
use Djiele\Memcached\Exception;

class Fnv132Hasher implements HasherInterface
{
    /**
     * fnv1_64 hashing algorithm
     * @param  string $string
     * @return int
     */
    public function hash($string)
    {
        if(! function_exists('memcached_hashkit_fnv1_32')) {
            throw new ConfigurationException('Module "memcached_hashkit" is not installed');
        }
        return memcached_hashkit_fnv1_32($string);
    }
}
