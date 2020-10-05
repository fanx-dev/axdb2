

using concurrent
using asyncServer

const class RActor : Actor {
  const ActorWorker worker := ActorWorker.fromActor(this)

  const Unsafe<RNode> node

  new make(File dir, Str name, Uri id) : super.make() {
    node = Unsafe(RNode(dir, name, id))
    this.sendLater(200ms, ["checkTimeout"])
  }

  protected override Obj? receive(Obj? msg) {
    if (worker.onReceive(msg)) return null
    
    Obj?[]? arg := msg
    Str method := arg[0]
    Obj?[]? args
    if (arg.size > 1) {
        args = arg[1]
    }
    
    //echo("RActor receive: $msg")
    
    if (method == "checkTimeout") {
        node.val.checkTimeout
        this.sendLater(200ms, ["checkTimeout"])
        return null
    }
    else {
        res := node.val.trap(method, args)
        return Unsafe(res)
    }
  }
}