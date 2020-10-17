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
  
  //private LogFile logFile
  internal LruCache cache
  
  internal File path
  internal Str name
  
  private Lock lock
  
//  private Int insertCount := 0
//  private Int commitPos := -1
  private Int logId := -1
  Int persistentId := -1 { private set }
  
  private StorageRes[] pool := [,]
  private Lock poolLock
  private Int poolSize
  private Bool isStop := false
  
  private MergeActor merger
  private AtomicBool busy := AtomicBool(false)
  
  new make(File path, Str name) {
    this.path = path
    this.name = name
    
    cacheSize := this.typeof.pod.config("cacheSize", "100000").toInt
    this.cache = LruCache(cacheSize)
    this.skipList = SkipList()
    //this.logFile = LogFile(path, name, skipList)
    this.lock = Lock()
    
    poolSize = 10
    poolSize.times {
        pool.add(StorageRes(this))
    }
    poolLock = Lock()
    
    //pool[0]->btree->dump
    
    merger = MergeActor(this)
//    merger.mergeLater
    this.persistentId = pool.first.logId
    
    echo("skipList:$skipList.size")
  }
  
  Void merge() {
    busy.val = true
    merger.mergeLater(0sec)
  }
  
  Bool isBusy() {
    busy.val
  }
  
  internal Int beginMerge() {
    id := lock.sync {
        //busy.val = true
        //logFile.reset
        immSkipList = skipList
        skipList = SkipList()
        lret logId
    }
    echo("begin merge")
    return id
  }
  
  internal Void endMerge(Int logId) {
    poolLock.sync {
        isStop = true
        lret null
    }
    
//    lock.sync {
//        logFile.reset
//        lret null
//    }
    
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
    persistentId = logId
    busy.val = false
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
  
  Void insert(BKey key, Int logId := 0) {
    lock.sync {
        //id = insertCount
        //++insertCount
        this.logId = logId
        skipList.insert(key)
        //logFile.write(key)
        lret null
    }
  }
  
//  Void commit(Int id) {
//    lock.sync {
//        if (commitPos < id) {
//            logFile.flush
//            commitPos = insertCount-1
//        }
//        lret null
//    }
//  }
  
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
    
    internal Int logId() {
        id := btree.store.meta["logId"]
        return id == null ? -1 : id.toInt
    }
    
    internal Array<Int8>? find(BKey key) {
        //val := storage.skipList.find(key)
        //if (val != null) return val
        return btree.search(key)
    }
}
