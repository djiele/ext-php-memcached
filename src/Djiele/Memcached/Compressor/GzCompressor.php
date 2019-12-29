<?php


namespace Djiele\Memcached\Compressor;


class GzCompressor implements ICompressor
{
    public function compress($data, array $options = [])
    {
        if (isset($options['level'])) {
            $level = $options['level'];
        } else {
            $level = 6;
        }
        if (isset($options['encoding'])) {
            $encoding = $options['encoding'];
        } else {
            $encoding = ZLIB_ENCODING_DEFLATE;
        }

        return gzcompress($data, $level, $encoding);
    }

    public function decompress($data)
    {
        return gzuncompress($data);
    }
}