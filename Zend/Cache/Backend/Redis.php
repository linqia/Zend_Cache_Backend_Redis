<?php

/**
 * @see Zend_Cache_Backend_Interface
 */
require_once 'Zend/Cache/Backend/ExtendedInterface.php';

/**
 * @see Zend_Cache_Backend
 */
require_once 'Zend/Cache/Backend.php';


/**
 * @package    Zend_Cache
 * @subpackage Zend_Cache_Backend
 * @copyright  Starfish
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */
class Zend_Cache_Backend_Redis extends Zend_Cache_Backend implements Zend_Cache_Backend_ExtendedInterface
{
    /**
     * Default Values
     */
    const DEFAULT_HOST = '127.0.0.1';
    const DEFAULT_PORT =  6379;
    const DEFAULT_TIMEOUT = 0;
    const DEFAULT_PERSISTENT = false;
    const DEFAULT_DB = 0;
    const DEFAULT_PREFIX = '';

    /**
     * Log message
     */
    const TAGS_UNSUPPORTED_BY_CLEAN_OF_REDIS_BACKEND = 'Zend_Cache_Backend_Redis::clean() : tags are unsupported by the Redis backend';
    const TAGS_UNSUPPORTED_BY_SAVE_OF_REDIS_BACKEND = 'Zend_Cache_Backend_Redis::save() : tags are unsupported by the Redis backend';
    const FILLING_PERCENTAGE_UNSUPPORTED_OF_REDIS_BACKEND = 'Zend_Cache_Backend_Redis::getFillingPercentage() : method unsupported by the Redis backend';

    /**
     * Available options
     *
     * 'host' => (string) : the name of the Redis server
     * 'port' => (int) : the port of the Redis server
     * 'socket' => (string) : the socket path for UNIX socket connections
     * 'timeout' => (int) : value in seconds which will be used for connecting to the daemon. Think twice
     *                      before changing the default value of 0 seconds - the connection to the backend
     *                      could be abruptedly closed if the script execution takes more than the timeout
     *                      you set.
     * 'persistent' => (bool) : use or not persistent connections to this Redis server
     * 'db' => (int) : database number to be used
     * 'prefix' => (string) : the prefix for cache keys
     *
     * @var array available options
     */
    protected $_options = array(
        'host'        => self::DEFAULT_HOST,
        'port'        => self::DEFAULT_PORT,
        'timeout'     => self::DEFAULT_TIMEOUT,
        'persistent'  => false,
        'db'          => self::DEFAULT_DB,
        'prefix'      => self::DEFAULT_PREFIX,
    );

    /**
     * Redis object
     *
     * @var Redis object
     */
    protected $_redis = null;

    /**
     * Redis backend connected
     * 
     * @var boolean
     */
    protected $_connected = false;

    /**
     * Constructor
     *
     * @param array $options associative array of options
     * @throws Zend_Cache_Exception
     * @return void
     */
    public function __construct(array $options = array())
    {
        if (!extension_loaded('redis')) {
            Zend_Cache::throwException('The redis extension must be loaded for using this backend !');
        }
        $this->_redis = new Redis;
        parent::__construct($options);
    }

    /**
     * Test if a cache is available for the given id and (if yes) return it (false else)
     *
     * @param  string  $id                     Cache id
     * @param  boolean $doNotTestCacheValidity If set to true, the cache validity won't be tested
     * @return string|false cached datas
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $this->_connect();
        $tmp = $this->_redis->get($id);
        if (is_array($tmp) && isset($tmp[0])) {
            return $tmp[0];
        }
        return false;
    }

    /**
     * Test if a cache is available or not (for the given id)
     *
     * @param  string $id Cache id
     * @return mixed|false (a cache is not available) or "last modified" timestamp (int) of the available cache record
     */
    public function test($id)
    {
        $this->_connect();
        $tmp = $this->_redis->get($id);
        if (is_array($tmp)) {
            return $tmp[1];
        }
        return false;
    }

    /**
     * Save some string datas into a cache record
     *
     * Note : $data is always "string" (serialization is done by the
     * core not by the backend)
     *
     * @param  string $data             Datas to cache
     * @param  string $id               Cache id
     * @param  array  $tags             Array of strings, the cache record will be tagged by each string entry
     * @param  int    $specificLifetime If != false, set a specific lifetime for this cache record (null => infinite lifetime)
     * @return boolean True if no problem
     */
    public function save($data, $id, $tags = array(), $specificLifetime = false)
    {
        $this->_connect();
        $lifetime = $this->getLifetime($specificLifetime);

        if ($lifetime > 0) {
            // Expire needs to be set
            $result = $this->_redis->setex($id, $lifetime, array($data, time(), $lifetime));
        } else {
            // Non-expiring entry
            $result = $this->_redis->set($id, array($data, time(), $lifetime));
        }

        if (count($tags) > 0) {
            $this->_log(self::TAGS_UNSUPPORTED_BY_SAVE_OF_REDIS_BACKEND);
        }

        return $result;
    }

    /**
     * Remove a cache record
     *
     * @param  string $id Cache id
     * @return boolean True if no problem
     */
    public function remove($id)
    {
        return (bool)$this->_redis->del($id);
    }

    /**
     * Clean some cache records
     *
     * Available modes are :
     * 'all' (default)  => remove all cache entries ($tags is not used)
     * 'old'            => unsupported
     * 'matchingTag'    => unsupported
     * 'notMatchingTag' => unsupported
     * 'matchingAnyTag' => unsupported
     *
     * @param  string $mode Clean mode
     * @param  array  $tags Array of tags
     * @throws Zend_Cache_Exception
     * @return boolean True if no problem
     */
    public function clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array())
    {
        switch ($mode) {
            case Zend_Cache::CLEANING_MODE_ALL:
                return $this->_redis->flushDB();
                break;
            case Zend_Cache::CLEANING_MODE_OLD:
                $this->_log("Zend_Cache_Backend_Redis::clean() : CLEANING_MODE_OLD is unsupported by the Redis backend");
                break;
            case Zend_Cache::CLEANING_MODE_MATCHING_TAG:
            case Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
            case Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
                $this->_log(self::TAGS_UNSUPPORTED_BY_CLEAN_OF_REDIS_BACKEND);
                break;
            default:
                Zend_Cache::throwException('Invalid mode for clean() method');
                break;
        }
    }

    /**
     * Return true if the automatic cleaning is available for the backend
     *
     * @return boolean
     */
    public function isAutomaticCleaningAvailable()
    {
        return false;
    }

    /**
     * Return an array of stored cache ids
     *
     * @return array array of stored cache ids (string)
     */
    public function getIds()
    {
        return $this->_redis->keys('*');
    }

    /**
     * Return an array of stored tags
     *
     * @return array array of stored tags (string)
     */
    public function getTags()
    {
        $this->_log(self::TAGS_UNSUPPORTED_BY_SAVE_OF_REDIS_BACKEND);
        return array();
    }

    /**
     * Return an array of stored cache ids which match given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of matching cache ids (string)
     */
    public function getIdsMatchingTags($tags = array())
    {
        $this->_log(self::TAGS_UNSUPPORTED_BY_SAVE_OF_REDIS_BACKEND);
        return array();
    }

    /**
     * Return an array of stored cache ids which don't match given tags
     *
     * In case of multiple tags, a logical OR is made between tags
     *
     * @param array $tags array of tags
     * @return array array of not matching cache ids (string)
     */
    public function getIdsNotMatchingTags($tags = array())
    {
        $this->_log(self::TAGS_UNSUPPORTED_BY_SAVE_OF_REDIS_BACKEND);
        return array();
    }

    /**
     * Return an array of stored cache ids which match any given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of any matching cache ids (string)
     */
    public function getIdsMatchingAnyTags($tags = array())
    {
        $this->_log(self::TAGS_UNSUPPORTED_BY_SAVE_OF_REDIS_BACKEND);
        return array();
    }

    /**
     * Return the filling percentage of the backend storage
     *
     * @throws Zend_Cache_Exception
     * @return int integer between 0 and 100
     */
    public function getFillingPercentage()
    {
        Zend_Cache::throwException(self::FILLING_PERCENTAGE_UNSUPPORTED_OF_REDIS_BACKEND);
    }

    /**
     * Return an array of metadatas for the given cache id
     *
     * The array must include these keys :
     * - expire : the expire timestamp
     * - tags : a string array of tags
     * - mtime : timestamp of last modification time
     *
     * @param string $id cache id
     * @return array array of metadatas (false if the cache id is not found)
     */
    public function getMetadatas($id)
    {
        $this->_connect();
        $tmp = $this->_redis->get($id);
        if (is_array($tmp)) {
            $data = $tmp[0];
            $mtime = $tmp[1];
            if (!isset($tmp[2])) {
                // because this record is only with 1.7 release
                // if old cache records are still there...
                return false;
            }
            $lifetime = $tmp[2];
            return array(
                'expire' => $mtime + $lifetime,
                'tags' => array(),
                'mtime' => $mtime
            );
        }
        return false;
    }

    /**
     * Give (if possible) an extra lifetime to the given cache id
     *
     * @param string $id cache id
     * @param int $extraLifetime
     * @return boolean true if ok
     */
    public function touch($id, $extraLifetime)
    {
        $this->_connect();
        $tmp = $this->_redis->get($id);
        if (is_array($tmp)) {
            $data = $tmp[0];
            $mtime = $tmp[1];
            if (!isset($tmp[2])) {
                // because this record is only with 1.7 release
                // if old cache records are still there...
                return false;
            }
            $lifetime = $tmp[2];
            $newLifetime = $lifetime - (time() - $mtime) + $extraLifetime;
            if ($newLifetime <=0) {
                return false;
            }
            return $this->_redis->setex($id, $newLifetime, array($data, time(), $newLifetime));
        }
        return false;
    }

    /**
     * Return an associative array of capabilities (booleans) of the backend
     *
     * The array must include these keys :
     * - automatic_cleaning (is automating cleaning necessary)
     * - tags (are tags supported)
     * - expired_read (is it possible to read expired cache records
     *                 (for doNotTestCacheValidity option for example))
     * - priority does the backend deal with priority when saving
     * - infinite_lifetime (is infinite lifetime can work with this backend)
     * - get_list (is it possible to get the list of cache ids and the complete list of tags)
     *
     * @return array associative of with capabilities
     */
    public function getCapabilities()
    {
        return array(
            'automatic_cleaning' => false,
            'tags' => false,
            'expired_read' => false,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true
        );
    }

    /**
     * A protected method to connect to the backend if a connection is not
     * already established
     * 
     * @throw Zend_Cache_Exception
     */
    protected function _connect()
    {
        if (!$this->_connected) {
            try {
                $method = $this->_options['persistent'] ? 'pconnect' : 'connect';
                if ($this->_options['timeout'] > 0) {
                    $this->_redis->$method($this->_options['host'], $this->_options['port'], $this->_options['timeout']);
                } else {
                    $this->_redis->$method($this->_options['host'], $this->_options['port']);
                }
                if (!empty($this->_options['prefix'])) {
                    $this->_redis->setOption(Redis::OPT_PREFIX, $this->_options['prefix']);
                }
                $this->_redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_PHP);
                $this->_redis->select($this->_options['db']);
            } catch (RedisException $e) {
                Zend_Cache::throwException('Connection failed: '.$e->getMessage());
            }
        }
        $this->_connected = true;
    }

}
