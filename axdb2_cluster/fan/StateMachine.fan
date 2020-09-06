// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

using axdb2_store

**
** StateMachine
**
class StateMachine
{
    private Storage storage
    
    new make(File dir, Str name) {
        storage = Storage(dir, name)
    }

    internal Void apply(LogEntry e) {

    }

    internal Void insert(Str key, Array<Int8> val) {
      bkey := BKey(key.toUtf8)
      bkey.value = val
      storage.insert(bkey)
    }

    internal Void saveSnapshot() {

    }
    
    Array<Int8> find(Str key) {
      bkey := BKey(key.toUtf8)
      return storage.find(bkey)
    }
}
