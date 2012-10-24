/** @file cache.go
 *  @brief implement cache relate functions 
 *  @author Andrin(atrejo) Dalong CHENG (dalongc)
 *  @date 2012-10-23
 */
package cache

import (
  "container/list"
  "fmt"
  "sync"
  "time"
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
)

/** 
 *  @brief cache entry 
 */
type Entry struct {
  Granted bool
  LeaseTime time.Time
  LeaseDur time.Duration

  Queries *list.List
  Data interface{}
}

/** 
 *  @brief cache 
 */
type Cache struct {
  Map map[string]*Entry
  Lock sync.Mutex
}

/**@brief create a cache 
 * @param void 
 * @return *Cache 
 */
func NewCache() *Cache {
  var cache Cache

  cache.Map = make(map[string]*Entry)

  return &cache
}

/**@brief Get is the most important function for cache, it will first clear all 
 *        the expire entries, and then fetch the cache content or add the count 
 *        of wantlease flag
 * @param key 
 * @param GetArgs
 * @return interface{}(string or []string)
 * @return error 
 */
func (cache *Cache) Get(
    key string, args *storageproto.GetArgs) (interface{}, error) {
  var entry *Entry
  var valid bool
  var data interface{}

  fmt.Printf("Cache get: %s\n", key)

  cache.Lock.Lock()
  cache.ClearExpired()

  entry, valid = cache.Map[key]
  if !valid {
    entry = new(Entry)
    entry.Queries = list.New()
    entry.Queries.PushBack(time.Now())

    fmt.Printf("Cache entry: %+v\n", *entry)
    fmt.Printf("Queries: %+v\n", entry.Queries)
    cache.Map[key] = entry

    cache.Lock.Unlock()
    return "", lsplog.MakeErr("Not found.")
  }

  if entry.Granted {
    data = entry.Data
    cache.Lock.Unlock()

    return data, nil
  }

  entry.Queries.PushBack(time.Now())

  fmt.Printf("Cache entry: %v\n", *entry)

  if entry.Queries.Len() > storageproto.QUERY_CACHE_THRESH {
    fmt.Printf("QUERY_CACHE_THRESH reached. Asking for lease.\n")
    args.WantLease = true
  }

  cache.Lock.Unlock()

  return "", lsplog.MakeErr("Not in cache")
}

/**@brief delete entries older than the query threshhold 
 *         in the list of query times. 
 * @param void 
 * @return void 
 */
func (ent *Entry) Clean() {
  var elem *list.Element
  var dur time.Duration

  elem = ent.Queries.Front()
  for elem != nil {
    dur = time.Since(elem.Value.(time.Time))
    if dur > (time.Duration(storageproto.QUERY_CACHE_SECONDS) * time.Second) {
      _ = ent.Queries.Remove(elem)
      elem = ent.Queries.Front()
    } else {
      break
    }
  }

}

/**@brief delete expire entries 
 * @param void 
 * @return void 
 */
func (cache *Cache) ClearExpired() {
  var key string
  var entry *Entry
  var dur time.Duration

  for key = range cache.Map {
    entry = cache.Map[key]

    if entry.Granted {
      dur = time.Since(entry.LeaseTime)
      if dur > entry.LeaseDur {
        fmt.Printf("Lease expired: %s\n", key)
        entry.Granted = false
      }
    }

    entry.Clean()

    fmt.Printf("No recent queries: %s\n", key)
    if entry.Queries.Len() == 0 {
      delete(cache.Map, key)
    }
  }
}

/**@brief invalidate certain entry 
 * @param string  
 * @return bool 
 */
func (cache *Cache) ClearEntry(key string) bool {
  var entry *Entry
  var valid bool

  cache.Lock.Lock()

  entry, valid = cache.Map[key]
  if valid {
    entry.Granted = false
  }

  cache.Lock.Unlock()

  return valid
}

/**@brief store the 'hot' content into cache, this function will be   
 *        used by revoke in libstore 
 * @param string 
 * @param interface{} 
 * @param LeaseStuct
 * @return void 
 */
func (cache *Cache) LeaseGranted(
    key string, data interface{}, lease storageproto.LeaseStruct) {
  var entry *Entry
  var valid bool

  cache.Lock.Lock()
  fmt.Printf("Lease granted: %s (%v)\n", key, data)

  entry, valid = cache.Map[key]
  if !valid {
    entry = new(Entry)
    entry.Queries = list.New()

    cache.Map[key] = entry
  }

  entry.Granted = true
  entry.LeaseTime = time.Now()
  entry.LeaseDur = time.Duration(lease.ValidSeconds) * time.Second

  cache.Lock.Unlock()
}
