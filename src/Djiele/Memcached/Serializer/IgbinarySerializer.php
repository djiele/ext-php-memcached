<?php


namespace Djiele\Memcached\Serializer;

class IgbinarySerializer implements ISerializer
{
    public function serialize($value)
    {
        return igbinary_serialize($value);
    }

    public function deserialize($value)
    {
        return igbinary_unserialize($value);
    }
}