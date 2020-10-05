

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
    reqStr := uri.query["req"]
    
    echo("onHttpService:$uri, path:$path")
    if (path == "execute") {
        f := await actor.send([path, reqStr, 1])
        rc := await (f.get as Unsafe).val
        
        buf2 := NioBuf.makeMem(1024)
        buf2.printLine("$rc")
        buf2.flip
        await res.writeFixed(buf2)
        return
    }

    buf := BufCrypto.fromBase64(reqStr)
    Obj? arg := buf.in.readObj
    args := [path, arg]

    f2 := await actor.send(args)
    await (f2.get as Unsafe).val
  }

}