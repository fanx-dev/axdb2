

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
    if (path == "execute" || path == "dump" || path == "appendEntries" || path == "requestVote") {
        //await Future
        f := await actor.send(uri)
        //await Promise
        rc := await (f.get as Unsafe).val
        
        buf1 := NioBuf.makeMem(1024)
        buf1.out.writeObj(rc)
        buf1.flip
        //echo("req: $uri, res:$rc")
        await res.writeFixed(buf1)
        return
    }
    else {
        echo("onHttpService:$uri")
        buf2 := NioBuf.makeMem(1024)
        buf2.printLine("HelloWorld:$uri")
        buf2.flip
        await res.writeFixed(buf2)
        return
    }
  }

}