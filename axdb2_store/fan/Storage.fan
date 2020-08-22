//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//
using concurrent


**
** Storage
**
class Storage
{
  internal SkipList skipList
  internal SkipList? immSkipList
  
  private LogFile logFile
  internal LruCache cache
  
  internal File path
  internal Str name
  
  private Lock lock
  
  private Int insertCount := 0
  private [Int:Obj?] commitGroup := [:]
  
  private StorageRes[] pool := [,]
  private Lock poolLock
  private Int poolSize
  private Bool isStop := false
  
  private MergeActor merger
  
  new make(File path, Str name) {
    this.path = path
    this.name = name
    this.cache = LruCache(1000)
    this.skipList = SkipList()
    this.logFile = LogFile(path, name, skipList)
    this.lock = Lock()
    
    poolSize = 10
    poolSize.times {
        pool.add(StorageRes(this))
    }
    poolLock = Lock()
    
    merger = MergeActor(this)
    merger.mergeLater
  }
  
  Void mergeNow() {
    merger.mergeLater(0sec)
  }
  
  internal Void beginMerge() {
    lock.sync {
        logFile.reset
        immSkipList = skipList
        skipList = SkipList()
        lret null
    }
    echo("begin merge")
  }
  
  internal Void endMerge(PageMgr writeStore) {
    poolLock.sync {
        isStop = true
        lret null
    }
    
    lock.sync {
        logFile.reset
        lret null
    }
    
    while (true) {
        safe := poolLock.sync {
            pool.size == poolSize
        }
        if (safe) break
    }
    
    poolLock.sync {
        pool.each { it.reload }
        cache.clear
        isStop = false
        lret null
    }
    echo("end merge")
  }
  
  private StorageRes getRes() {
    while (true) {
        res := poolLock.sync {
            if (isStop) lret null
            lret pool.pop
        }
        if (res != null) return res
    }
    throw Err("unreachable")
  }
  
  Array<Int8>? find(BKey key) {
    val := skipList.find(key)
    if (val != null) return val
    
    res := getRes
    try {
        val = res.find(key)
    }
    finally {
        poolLock.sync { pool.push(res); lret null }
    }
    return val
  }
  
  Void insert(BKey key) {
    id := 0
    lock.sync {
        id = insertCount
        ++insertCount
        skipList.insert(key)
        logFile.write(key)
        
        commitGroup[id] = null
        lret null
    }
    //Actor.sleep(1ms)
    
    lock.sync {
        if (commitGroup.containsKey(id)) {
            logFile.flush
            commitGroup.clear
        }
        lret null
    }
  }
  
}

internal class StorageRes {
    private Storage storage
    private BTree btree
    
    new make(Storage storage) {
        this.storage = storage
        store := PageMgr(storage.path, storage.name, true)
        btree = BTree(store, "table1", storage.cache)
    }
    
    internal Void reload() {
        btree.reload
    }
    
    internal Array<Int8>? find(BKey key) {
        val := storage.skipList.find(key)
        if (val != null) return val
        return btree.search(key)
    }
}
