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
    
    override Bool isBusy() { storage.isBusy }
    
    override Int snapshotPoint() { storage.persistentId }
    
    override Tuple<Array<Int8>, Int>? snapshotChunk(Int offset) {
      fileId := 0
      pos := 0
      while (true) {
        file := (storage.path+`${storage.name}_${fileId}.dat`)
        if (!file.exists) break
        
        if (offset > pos && offset < (pos + file.size) ) {
            buf := file.open
            buf.seek(offset - pos)
            data := Array<Int8>(8*1024)
            n := buf.in.readBytes(data)
            buf.close
            if (n > 0) {
                return Tuple<Array<Int8>, Int>(data, fileId)
            }
        }
        else {
            pos += file.size
        }
        ++fileId
      }
      file := (storage.path+`${storage.name}.meta`)
      buf := file.open
      data := Array<Int8>(file.size)
      n := buf.in.readBytes(data)
      buf.close
      return Tuple<Array<Int8>, Int>(data, -1)
    }
    
    override Void set(Str key, Str? val, Int logId) {
        bkey := BKey(key.toUtf8)
        bkey.value = val == null ? null : val.toUtf8
        storage.insert(bkey, logId)
    }
    
    override Str? get(Str key) {
        bkey := BKey(key.toUtf8)
        bval := storage.find(bkey)
        if (bval == null) return null
        return Str.fromUtf8(bval)
    }
}
