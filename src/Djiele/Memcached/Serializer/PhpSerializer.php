<?php


namespace Djiele\Memcached\Serializer;

class PhpSerializer implements ISerializer
{
    public function serialize($value)
    {
        return serialize($value);
    }

    public function deserialize($value)
    {
        return unserialize($value);
    }
}