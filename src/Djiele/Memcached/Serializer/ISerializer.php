<?php


namespace Djiele\Memcached\Serializer;

interface ISerializer
{
    /**
     * Serialize given value
     * @param $value
     * @return mixed
     */
    public function serialize($value);

    /**
     * Unserialize previously serialized value
     * @param $value
     * @return mixed
     */
    public function deserialize($value);
}