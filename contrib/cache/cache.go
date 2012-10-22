package cache

import (
  "container/list"
  "fmt"
  "time"
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
)

const (
  CACHE_LEN = 100
  CACHE_THRESH = 5
)

type CacheEntry struct {
  isValid bool
  isInCache bool
  queryTime time.Time
  queries *list.List
  queryCount int
  key string
  data interface{}
}

type Cache struct {
  entries []CacheEntry
}

func NewCache() (*Cache, error) {
  var cache Cache

  cache.entries = make([]CacheEntry, CACHE_LEN)

  return &cache, nil
}

/**
 * Delete entries older than the query threshhold in the list of query times.
 * */
func (entry *CacheEntry) Clean() {
  var d time.Duration
  var e *list.Element

  e = entry.queries.Front()
  for e != nil {
    d = time.Since(e.Value.(time.Time))
    if d > storageproto.QUERY_CACHE_SECONDS {
      _ = entry.queries.Remove(e)
      e = entry.queries.Front()
    } else {
      break
    }
  }

  if entry.queries.Len() > storageproto.QUERY_CACHE_THRESH {
    fmt.Printf("QUERY_CACHE_THRESH reached. Asking for lease.\n")
  }
}

func (entry *CacheEntry) timeDiff() int {
  return 0
}

func (c *Cache) Get(key string, args *storageproto.GetArgs)(interface{}, error){
  for i := 0; i < len(c.entries); i++ {
    if c.entries[i].isValid && key == c.entries[i].key {
      if c.entries[i].isInCache {
        return c.entries[i].data, nil
      }

      c.entries[i].queries.PushBack(time.Now())
      c.entries[i].Clean()

      if c.entries[i].queries.Len() >= storage.QUERY_CACHE_THRESH {
        args.WantLease = true
      }

      if c.entries[i].timeDiff() < CACHE_THRESH {
        c.entries[i].queryCount++
      } else {
        c.entries[i].queryCount = 1
      }

      if c.entries[i].queryCount == storageproto.QUERY_CACHE_THRESH {
        args.WantLease = true
      }
      return "", lsplog.MakeErr("Not in cache")
    }
  }

  c.clearExpireEntries()
  c.insert(key)

  return "", nil
}

func (c *Cache) insert(key string) {
  var entry = CacheEntry{true, false, time.Now(), list.New(), 1, key, nil}
  entry.queries.PushBack(time.Now())

  for i := 0; i < len(c.entries); i++ {
    if !c.entries[i].isValid {
      c.entries[i] = entry
      return
    }
  }

  c.entries = append(c.entries, entry)
}

func (c *Cache) clearExpireEntries() {
  for i := 0; i < len(c.entries); i++ {
    if c.entries[i].isValid && (c.entries[i].timeDiff() >= CACHE_THRESH) {
      c.entries[i].isValid = false
    }
  }
}

func (c *Cache) ClearEntry(key string) error {
  for i := 0; i < len(c.entries); i++ {
    if c.entries[i].isValid &&c.entries[i].key == key &&c.entries[i].isInCache{
      c.entries[i].isValid = false
      return nil
    }
  }

  return lsplog.MakeErr("Key not in cache !")
}
