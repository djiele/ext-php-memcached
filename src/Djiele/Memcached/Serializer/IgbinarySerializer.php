<?php


namespace Djiele\Memcached\Serializer;

use Djiele\Memcached\Exception;

class IgbinarySerializer implements ISerializer
{
    public function serialize($value)
    {
        if(! function_exists('igbinary_serialize')) {
            throw new ConfigurationException('Module "igbinary" is not installed');
        }
        return igbinary_serialize($value);
    }

    public function deserialize($value)
    {
        if(! function_exists('igbinary_unserialize')) {
            throw new ConfigurationException('Module "igbinary" is not installed');
        }
        return igbinary_unserialize($value);
    }
}