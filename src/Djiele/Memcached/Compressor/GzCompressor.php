<?php


namespace Djiele\Memcached\Compressor;

use Djiele\Memcached\Exception;

class GzCompressor implements ICompressor
{
    public function compress($data, array $options = [])
    {
        if(! function_exists('gzcompress')) {
            throw new ConfigurationException('Module "zlib" is not installed');
        }
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
        if(! function_exists('gzuncompress')) {
            throw new ConfigurationException('Module "zlib" is not installed');
        }
        return gzuncompress($data);
    }
}