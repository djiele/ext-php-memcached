<?php


namespace Djiele\Memcached\Compressor;


class FastlzCompressor
{
    public function compress($data, array $options = [])
    {
        if (isset($options['level']) && in_array($options['level'], [1, 2])) {
            $level = $options['level'];
        } else {
            $level = 1;
        }

        return fastlz_compress($data, $level);
    }

    public function decompress($data)
    {
        return fastlz_decompress($data);
    }
}