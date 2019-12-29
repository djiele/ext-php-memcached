<?php


namespace Djiele\Memcached\Compressor;


interface ICompressor
{
    /**
     * Compress given value
     * @param $value
     * @param array $options
     * @return mixed
     */
    public function compress($value, array $options = []);

    /**
     * Decompress previously compressed value
     * @param $compressedValue
     * @return mixed
     */
    public function decompress($compressedValue);
}