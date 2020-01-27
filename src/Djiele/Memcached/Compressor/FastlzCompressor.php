<?php


namespace Djiele\Memcached\Compressor;

use Djiele\Memcached\Exception;

class FastlzCompressor
{
    public function compress($data, array $options = [])
    {
        if(! function_exists('fastlz_compress')) {
            throw new ConfigurationException('Module "fastlz" is not installed');
        }
        if (isset($options['level']) && in_array($options['level'], [1, 2])) {
            $level = $options['level'];
        } else {
            $level = 1;
        }

        return fastlz_compress($data, $level);
    }

    public function decompress($data)
    {
        if(! function_exists('fastlz_decompress')) {
            throw new ConfigurationException('Module "fastlz" is not installed');
        }
        return fastlz_decompress($data);
    }
}