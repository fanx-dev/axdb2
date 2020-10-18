

using asyncServer
using concurrent

const class RpcServer : HttpHandler {

  const RActor actor

  new make(File dir, Str name, Uri id) {
    actor = RActor(dir, name, id)
  }

  override async Void onHttpService(HttpReq req, HttpRes res) {
    uri := req.uri
    
    path := uri.path[0]
    if (path == "execute" || path == "dump" || path == "appendEntries" || 
        path == "requestVote" || path == "installSnapshot") {
        
        Obj? arg := null
        if (req.method == "POST") {
            Buf buf := await req.read
            if (path == "appendEntries") {
                reqArg := AppendEntriesReq()
                reqArg.read(buf.in)
                arg = Unsafe(reqArg)
            }
            else if (path == "installSnapshot") {
                reqArg2 := InstallSnapshotReq()
                reqArg2.read(buf.in)
                arg = Unsafe(reqArg2)
            }
        }
        //echo("service: $uri, $arg")
        //await Future
        f := await actor.send([uri, arg])
        //await Promise
        rc := await (f.get as Unsafe).val
        
        buf1 := NioBuf.makeMem(1024)
        buf1.out.writeObj(rc)
        buf1.flip
        //echo("req: $uri, res:$rc")
        await res.writeFixed(buf1)
        return
    }
    else if (path == "find") {
        RNode node := actor.node.val
        key := uri.query["key"]
        val := node.stateMachine.get(key)
        buf2 := NioBuf.makeMem(1024)
        buf2.print(val)
        buf2.flip
        await res.writeFixed(buf2)
        return
    }
    else {
        echo("onHttpService:$uri")
        buf3 := NioBuf.makeMem(1024)
        buf3.printLine("HelloWorld:$uri")
        buf3.flip
        await res.writeFixed(buf3)
        return
    }
  }

}