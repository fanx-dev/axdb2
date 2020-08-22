//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//

**
** LruCache
**
class LruCache
{
  private Cache cache
  private Lock lock
  
  new make(Int size) {
    cache = Cache(size)
    lock = Lock()
  }
  
  Obj? get(Obj key) {
    lock.lock
    obj := cache.get(key)
    lock.unlock
    return obj
  }
  
  Void set(Obj key, Obj? val) {
    lock.lock
    cache.set(key, val)
    lock.unlock
  }
  
  Void remove(Obj key) {
    lock.lock
    cache.remove(key)
    lock.unlock
  }
  
  Void clear() {
    lock.lock
    cache.clear
    lock.unlock
  }
}
