// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//


**
** StateMachine
**
mixin StateMachine
{    
    Void apply(Str cmd, Int logId) {
        fs := cmd.splitBy(":", 2)
        key := fs[0]
        Str? value
        if (fs.size > 1) {
            value = fs[1]
        }
        set(key, value, logId)
    }
    
    protected abstract Void set(Str key, Str? val, Int logId)
    abstract Str? get(Str key)
    
    virtual Bool saveSnapshot() { false }
    virtual Int snapshotPoint() { -1 }
    virtual Bool isBusy() { true }
    virtual InstallSnapshotReq? snapshotChunk(Int offset) { null }
    
    virtual Void dump() {}
}

class MemStateMachine : StateMachine {
    private [Str:Str] map := [:]
    
    new make(File dir, Str name) {}
    
    override Void set(Str key, Str? val, Int logId) {
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
