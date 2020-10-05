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
mixin StateMachine
{    
    Void apply(Str cmd) {
        fs := cmd.splitBy(":", 2)
        key := fs[0]
        Str? value
        if (fs.size > 1) {
            value = fs[1]
        }
        set(key, value)
    }
    
    protected abstract Void set(Str key, Str? val)
    abstract Str? get(Str key)
    
    virtual Bool saveSnapshot() { false }
    
    virtual Void dump() {}
}

class MemStateMachine : StateMachine {
    private [Str:Str] map := [:]
    
    new make(File dir, Str name) {}
    
    override Void set(Str key, Str? val) {
        if (val == null) {
            map.remove(key)
            return
        }
        map[key] = val
    }
    
    override Str? get(Str key) {
        map[key]
    }
    
    override Void dump() {
        echo("StateMachine:$map")
    }
}

class StoreStateMachine {
    private Storage storage
    
    new make(File dir, Str name) {
        storage = Storage(dir, name)
    }

    internal Void set(Str key, Array<Int8> val) {
      bkey := BKey(key.toUtf8)
      bkey.value = val
      storage.insert(bkey)
    }

    internal Bool saveSnapshot() {
        //TODO
        return true
    }
    
    Array<Int8> get(Str key) {
      bkey := BKey(key.toUtf8)
      return storage.find(bkey)
    }
}
