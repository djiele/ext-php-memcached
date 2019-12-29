<?php


namespace Djiele\Memcached\DistributedHash;

use Flexihash\Exception;
use Flexihash\Hasher\HasherInterface;
use Flexihash\Hasher\Crc32Hasher;

class DistributedHashModula implements IDistributedHash
{
    protected $hasher = null;
    protected $replica = null;
    protected $targets = [];
    protected $countTargets = 0;

    /**
     * Constructor.
     * @param HasherInterface $hasher
     * @param null $replica
     */
    public function __construct(HasherInterface $hasher = null, $replica = null)
    {
        $this->hasher = $hasher ? $hasher : new Crc32Hasher();
        $this->replica = $replica;
    }

    /**
     * Add a target to the server pool
     * @param $target
     * @param int $weight
     * @return $this
     */
    public function addTarget($target, $weight = 1)
    {
        if (!in_array($target, $this->targets))
        {
            $this->targets[] = $target;
            ++$this->countTargets;
        }

        return $this;
    }

    /**
     * Add multiple targets to the server pool
     * @param array $targets
     * @param int $weight
     * @return $this
     */
    public function addTargets(array $targets, $weight = 1)
    {
        foreach ($targets as $target) {
            $this->addTarget($target, $weight);
        }

        return $this;
    }

    /**
     * Remove a target.
     * @param string $target
     * @return $this
     * @throws Exception when target does not exist
     */
    public function removeTarget($target)
    {
        if (!in_array($target, $this->targets)) {
            throw new Exception("Target '$target' does not exist.");
        }
        if (false !== ($index = array_search($target, $this->targets))) {
            unset($this->targets[$index]);
            $this->targets = array_values($this->targets);
            --$this->countTargets;
        }

        return $this;
    }

    /**
     * A list of all potential targets.
     * @return array
     */
    public function getAllTargets()
    {
        return $this->targets;
    }

    /**
     * Looks up the target for the given resource.
     * @param string $resource
     * @return string
     * @throws Exception when no targets defined
     */
    public function lookup($resource)
    {
        if (empty($this->targets)) {
            throw new Exception('No targets exist');
        }
        $h = $this->hasher->hash($resource);

        return $this->targets[$h % $this->countTargets];
    }

    /**
     * Get a list of targets for the resource, in order of precedence.
     * Up to $requestedCount targets are returned, less if there are fewer in total.
     *
     * @param string $resource
     * @param int $requestedCount The length of the list to return
     * @return array List of targets
     * @throws Exception when count is invalid
     */
    public function lookupList($resource, $requestedCount)
    {
        $ret = [];
        if (!$requestedCount) {
            throw new Exception('Invalid count requested');
        }
        // handle no targets
        if (empty($this->targets)) {
            return [];
        }
        // optimize single target
        if (1 == $this->countTargets) {
            return $this->targets[0];
        }
        $index = $i = array_search($this->lookup($resource), $this->targets);
        for (; $i < $this->countTargets; $i++) {
            if (!in_array($this->targets[$i], $ret)) {
                $ret[] = $this->targets[$i];
                if ($requestedCount == count($ret)) {
                    break;
                }
            }
        }
        if($requestedCount > count($ret)) {
            for ($i = 0; $i < $index; $i++) {
                if (!in_array($this->targets[$i], $ret)) {
                    $ret[] = $this->targets[$i];
                    if ($requestedCount == count($ret)) {
                        break;
                    }
                }
            }
        }

        return $ret;
    }
}