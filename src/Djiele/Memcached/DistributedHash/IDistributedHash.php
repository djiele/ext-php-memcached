<?php


namespace Djiele\Memcached\DistributedHash;


use Flexihash\Exception;
use Flexihash\Hasher\HasherInterface;

interface IDistributedHash
{
    /**
     * Constructor.
     * @param HasherInterface $hasher
     * @param null $replica
     */
    public function __construct(HasherInterface $hasher = null, $replica = null);

    /**
     * Add a target to the server pool
     * @param $target
     * @param int $weight
     * @return $this
     */
    public function addTarget($target, $weight = 1);

    /**
     * Add multiple targets to the server pool
     * @param array $targets
     * @param int $weight
     * @return $this
     */
    public function addTargets(array $targets, $weight = 1);

    /**
     * Remove a target.
     * @param string $target
     * @return $this
     * @throws Exception when target does not exist
     */
    public function removeTarget($target);

    /**
     * A list of all potential targets.
     * @return array
     */
    public function getAllTargets();

    /**
     * Looks up the target for the given resource.
     * @param string $resource
     * @return string
     * @throws Exception when no targets defined
     */
    public function lookup($resource);

    /**
     * Get a list of targets for the resource, in order of precedence.
     * Up to $requestedCount targets are returned, less if there are fewer in total.
     * @param string $resource
     * @param int $requestedCount The length of the list to return
     * @return array List of targets
     * @throws Exception when count is invalid
     */
    public function lookupList($resource, $requestedCount);
}