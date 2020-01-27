# Memcached
This library is a native PHP emulation of memcached PHP extension. The MemcachedClient class implements all of the Memcached ASCII protocol (but not the meta commands yet) and the binary protocol with SASL authentication.

Note for windows users: this library makes use of few PHP extensions (igbinary, msgpack, fastlz, memcached_hashkit). Although these extensions are not mandatory, they should be used to reproduce the default configuration of the php_memcached extension.

PHP_Fastlz can be downloaded from [here](https://www.djiele.net/php-fastlz/)

PHP_Memcached_hashkit can be downloaded from [here](https://www.djiele.net/php-memcached-hashkit/)

##### Installation

You can install the package via composer:

```
composer require djiele/ext-php-memcached "dev-master"
```

##### Simple usage


```php
require_once __DIR__'./vendor/autoload.php';
use Djiele\Memcached\Memcached;
$memc = new Memcached();
$memc->addServer('127.0.0.1', 11211, 80);
$memc->add('key', 'value', 3600);
$var = $memc->get('key', function(Memcached $m, $k, &$v) { $v = uniqid(); return true; }, Memcached::GET_EXTENDED), true);
var_dump($var);
```

##### Features
See the [manual](https://www.php.net/manual/en/book.memcached.php) for functions  references
```php
__construct
add
addByKey
addServer
addServers
append
appendByKey
cas
casByKey
decrement
decrementByKey
delete
deleteByKey
deleteMulti
deleteMultiByKey
fetch
fetchAll
flush
get
getAllKeys
getByKey
getDelayed
getDelayedByKey
getMulti
getMultiByKey
getOption
getResultCode
getResultMessage
getServerByKey
getServerList
getStats
getVersion
increment
incrementByKey
isPersistent
isPristine
prepend
prependByKey
quit
replace
replaceByKey
resetServerList
set
setByKey
setCredentials
setMulti
setMultiByKey
setOption
setOptions
setSaslAuthData
touch
touchByKey
```

