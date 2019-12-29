<?php


namespace Djiele\Memcached\Serializer;


class MsgpackSerializer implements ISerializer
{
    public function serialize($value)
    {
        return msgpack_pack($value);
    }

    public function deserialize($value)
    {
        return msgpack_unpack($value);
    }
}