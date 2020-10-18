

using concurrent
using asyncServer

const class RActor : Actor {
  const ActorWorker worker := ActorWorker.fromActor(this)

  const Unsafe<RNode> node
  
  static const Duration checkTime = 50ms

  new make(File dir, Str name, Uri id) : super.make() {
    node = Unsafe(RNode(dir, name, id))
    this.sendLater(1sec, "checkTimeout")
  }

  protected override Obj? receive(Obj? msg) {
    if (worker.onReceive(msg)) return null
    
    if (msg is List) {
        list := msg as List
        uri := list[0] as Uri
        argUnsafe := list[1] as Unsafe

        //echo("RActor receive:$uri")
        method := uri.path[0]
        Obj?[]? args
        if (method == "execute") {
            cmd := uri.query["cmd"]
            typeStr := uri.query["type"]
            sync := uri.query["sync"] == "true"
            type := typeStr == null ? 0 : typeStr.toInt
            //echo("$uri,$typeStr,$type")
            args = [cmd, type, sync]
        }
        else {
            reqStr := uri.query["req"]
            if (reqStr != null) {
                //buf := BufCrypto.fromBase64(reqStr)
                Obj? arg := reqStr.in.readObj
                args = [arg]
            }
        }
        
        if (argUnsafe != null) {
            args = [argUnsafe.val]
        }
        
        try {
            res := node.val.trap("on"+method.capitalize, args)
            return Unsafe(res)
        }
        catch (Err e) {
            echo("ERROR: call on$method, $args")
            e.trace
        }
    }
    else if (msg == "checkTimeout") {
        node.val.checkTimeout
        this.sendLater(checkTime, "checkTimeout")
        return null
    }
    
    throw Err("Unknow msg: $msg")
  }
}