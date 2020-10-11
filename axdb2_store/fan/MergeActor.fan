//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//
using concurrent


abstract const class BaseActor : Actor {
  //protected static const Str key := "axdb.store."

  new make() : super(ActorPool{maxThreads=1}) {}

  protected override Obj? receive(Obj? msg) {
    Obj?[]? arg := msg
    Str name := arg[0]
    Obj?[]? args := arg[1]

    //log.debug("receive $msg")

    try {
      return trap(name, args)
    } catch (Err e) {
      e.trace
      throw e
    }
  }

  override Obj? trap(Str name, Obj?[]? args := null) {
    if (name.startsWith("send_")) {
      method := name[5..-1]
      return this.send([method, args].toImmutable)
    }
    return super.trap(name, args)
  }
}

const class MergeActor : BaseActor {
    private const Unsafe<Storage> storage
    private const Unsafe<BTree> btree
    
    new make(Storage storage) {
        this.storage = Unsafe(storage)
        store := PageMgr(storage.path, storage.name)
        btree = Unsafe(BTree(store, "table1", storage.cache))
    }
    
    internal Void mergeLater(Duration delay := 20sec) {
        this.sendLater(delay, ["merge", null].toImmutable)
    }

    private Void merge() {
        //echo("try merge")
        if (storage.val.skipList.size == 0) {
            //mergeLater(20sec)
            return
        }
        logId := storage.val.beginMerge
        changes := storage.val.immSkipList.list
        storage.val.immSkipList = null
        btree.val.insertAll(changes, logId)
        storage.val.endMerge(logId)
        //mergeLater
    }
}