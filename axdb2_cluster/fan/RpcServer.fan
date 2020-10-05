

using asyncServer

const class RpcServer : HttpHandler {

  const RActor actor

  new make(File dir, Str name, Uri id) {
    actor = RActor(dir, name, id)
  }

  override async Void onHttpService(HttpReq req, HttpRes res) {
    //echo("Server receive: "+req.headers)
    // res.headers["Content-Type"] = "text/html; charset=utf-8"

    // buf := NioBuf.makeMem(1024)
    // buf.printLine("<html>
    //                     <body>Hello World</body>
    //                    </html>")
    // buf.flip
    //await res.writeFixed(buf)
    //await res.writeChunk(buf)

    uri := req.uri
    path := uri.pathStr

    reqStr := uri.query["req"]
    buf := BufCrypto.fromBase64(reqStr)

    Obj? arg
    if (path == "appendEntries") {
      arg = AppendEntriesReq.read(buf.in)
    }
    else {
      arg = buf.in.readObj
    }
    args := [path, arg]

    await actor.send(args)
  }

}