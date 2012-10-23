package cache

import (
  "container/list"
  "fmt"
  "sync"
  "time"
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
)

type Entry struct {
  Granted bool
  LeaseTime time.Time
  LeaseDur time.Duration

  Queries *list.List
  Data interface{}
}

type Cache struct {
  Map map[string]*Entry
  Lock sync.Mutex
}

func NewCache() *Cache {
  var cache Cache

  cache.Map = make(map[string]*Entry)

  return &cache
}

/**
 * Delete entries older than the query threshhold in the list of query times.
 * */
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

  entry.Queries.PushBack(time.Now())

  if entry.Granted {
    data = entry.Data
    cache.Lock.Unlock()

    return data, nil
  }

  fmt.Printf("Cache entry: %v\n", *entry)

  if entry.Queries.Len() > storageproto.QUERY_CACHE_THRESH {
    fmt.Printf("QUERY_CACHE_THRESH reached. Asking for lease.\n")
    args.WantLease = true
  }

  cache.Lock.Unlock()

  return "", lsplog.MakeErr("Not in cache")
}

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
