<?php


namespace Djiele\Memcached\Serializer;

use Djiele\Memcached\Exception;

class MsgpackSerializer implements ISerializer
{
    public function serialize($value)
    {
        if(! function_exists('msgpack_pack')) {
            throw new ConfigurationException('Module "msgpack" is not installed');
        }
        return msgpack_pack($value);
    }

    public function deserialize($value)
    {
        if(! function_exists('msgpack_unpack')) {
            throw new ConfigurationException('Module "msgpack" is not installed');
        }
        return msgpack_unpack($value);
    }
}