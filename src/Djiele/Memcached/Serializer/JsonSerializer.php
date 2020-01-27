<?php


namespace Djiele\Memcached\Serializer;

use Djiele\Memcached\Exception;

class JsonSerializer implements  ISerializer
{
    public function serialize($value)
    {
        if(! function_exists('json_encode')) {
            throw new ConfigurationException('Module "json" is not installed');
        }
        return json_encode($value);
    }

    public function deserialize($value)
    {
        if(! function_exists('json_decode')) {
            throw new ConfigurationException('Module "json" is not installed');
        }
        return json_decode($value);
    }
}