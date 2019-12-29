<?php


namespace Djiele\Memcached\Serializer;

class JsonSerializer implements  ISerializer
{
    public function serialize($value)
    {
        return json_encode($value);
    }

    public function deserialize($value)
    {
        return json_decode($value);
    }
}