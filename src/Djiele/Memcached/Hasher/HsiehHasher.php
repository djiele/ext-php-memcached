<?php

namespace Djiele\Memcached\Hasher;

use Flexihash\Hasher\HasherInterface;
use Djiele\Memcached\Exception;

class HsiehHasher implements HasherInterface
{
    /**
     * Hsieh hashing algorithm
     * @param  string $string
     * @return int
     */
    public function hash($string)
    {
        if(! function_exists('memcached_hashkit_hsieh')) {
            throw new ConfigurationException('Module "memcached_hashkit" is not installed');
        }
        return memcached_hashkit_hsieh($string);
    }
}
