// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-10-11 yangjiandong Creation
//

using axdb2_store

class StoreStateMachine : StateMachine {
    private Storage storage
    
    new make(File dir, Str name) {
        storage = Storage(dir, name)
    }

    override Bool saveSnapshot() {
        storage.merge
        return true
    }
    
    override Int snapshotPoint() { storage.persistentId }
    
    override Void set(Str key, Str? val, Int logId) {
        bkey := BKey(key.toUtf8)
        bkey.value = val == null ? null : val.toUtf8
        storage.insert(bkey, logId)
    }
    
    override Str? get(Str key) {
        bkey := BKey(key.toUtf8)
        bval := storage.find(bkey)
        return Str.fromUtf8(bval)
    }
}
