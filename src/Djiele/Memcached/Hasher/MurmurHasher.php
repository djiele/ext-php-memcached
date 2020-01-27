<?php

namespace Djiele\Memcached\Hasher;

use Flexihash\Hasher\HasherInterface;
use Djiele\Memcached\Exception;

class MurmurHasher implements HasherInterface
{
    /**
     * Murmur2 hashing algorithm
     * @param  string $string
     * @return int
     */
    public function hash($string)
    {
        if(! function_exists('memcached_hashkit_murmur')) {
            throw new ConfigurationException('Module "memcached_hashkit" is not installed');
        }
        return memcached_hashkit_murmur($string);
    }
}
