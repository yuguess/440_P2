package cache

import (
  "time"
  "P2-f12/official/storageproto"
  "P2-f12/official/lsplog"
)

const (
  CACHE_LEN = 100
  CACHE_THRESH = 5
)

type CacheEntry struct {
  isValid bool
  isInCache bool
  queryTime time.Time
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

func (entry *CacheEntry) timeDiff() int {
  return 0
}

func (c *Cache) Get(key string, args *storageproto.GetArgs)(interface{}, error){
  for i := 0; i < len(c.entries); i++ {
    if c.entries[i].isValid && key == c.entries[i].key {
      if c.entries[i].isInCache {
        return c.entries[i].data, nil
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
  var entry = CacheEntry{true, false, time.Now(), 1, key, nil}

  for i := 0; i < len(c.entries); i++ {
    if !c.entries[i].isValid {
      c.entries[i] = entry
      return
    }
  }

  c.entries = append(c.entries, []CacheEntry{entry}...)
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
