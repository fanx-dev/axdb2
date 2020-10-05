

using asyncServer
using concurrent

const class RpcServer : HttpHandler {

  const RActor actor

  new make(File dir, Str name, Uri id) {
    actor = RActor(dir, name, id)
  }

  override async Void onHttpService(HttpReq req, HttpRes res) {
    uri := req.uri
    echo("onHttpService:$uri")
    path := uri.path[0]
    
    if (path == "execute") {
        cmd := uri.query["cmd"]
        typeStr := uri.query["type"]
        type := typeStr == null ? 0 : typeStr.toInt
        //await Future
        f := await actor.send([path, [cmd, type]])
        //await Promise
        rc := await (f.get as Unsafe).val
        
        buf1 := NioBuf.makeMem(1024)
        buf1.printLine("$rc")
        buf1.flip
        await res.writeFixed(buf1)
        return
    }
    else if (path == "dump") {
        f2 := await actor.send([path, null])
        str := (f2.get as Unsafe).val
        buf2 := NioBuf.makeMem(1024)
        buf2.printLine(str)
        buf2.flip
        await res.writeFixed(buf2)
        return
    }
    else {
        reqStr := uri.query["req"]
        //buf := BufCrypto.fromBase64(reqStr)
        Obj? arg := reqStr.in.readObj
        args := [path, [arg]]

        f3 := await actor.send(args)
        await (f3.get as Unsafe).val
    }
  }

}