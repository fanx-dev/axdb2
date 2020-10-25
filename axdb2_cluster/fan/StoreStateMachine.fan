// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-10-11 yangjiandong Creation
//

using axdb2_store

class StoreStateMachine : StateMachine {
    private Storage storage
    
    new make(File dir) {
        storage = Storage(dir)
    }

    override Bool saveSnapshot() {
        storage.merge
        return true
    }
    
    override Bool isBusy() { storage.isBusy }
    
    override Int snapshotPoint() { storage.persistentId }
    
    override InstallSnapshotReq? snapshotChunk(Int offset) {
        fileId := 0
        pos := 0
        while (true) {
          file := (storage.path+`data-${fileId}.dat`)
          echo("$file, $file.exists")
          if (!file.exists) break

          if (offset >= pos && offset < (pos + file.size) ) {
              buf := file.open
              buf.seek(offset - pos)
              data := Array<Int8>(8*1024)
              n := buf.in.readBytes(data)
              buf.close
              if (n > 0) {
                  req := InstallSnapshotReq()
                  req.offset = offset
                  req.fileId = fileId
                  req.fileOffset = offset - pos
                  req.data = data
                  req.done = false
                  return req
              }
          }
          else {
              pos += file.size
          }
          ++fileId
        }
        file := (storage.path+`data.meta`)
        buf := file.open
        data := Array<Int8>(file.size)
        n := buf.in.readBytes(data)
        buf.close
      
        req := InstallSnapshotReq()
        req.offset = offset
        req.fileId = -1
        req.fileOffset = 0
        req.data = data
        req.done = true
        return req
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
