<?php

namespace Djiele\Memcached\Hasher;

use Flexihash\Hasher\HasherInterface;
use Djiele\Memcached\Exception;

class OneAtATimeHasher implements HasherInterface
{
    /**
     * Jenkin's One at a time hashing algorithm
     * @param  string $string
     * @return int
     */
    public function hash($string)
    {
        if(! function_exists('memcached_hashkit_one_at_a_time')) {
            throw new ConfigurationException('Module "memcached_hashkit" is not installed');
        }
        return memcached_hashkit_one_at_a_time($string);
    }
}
