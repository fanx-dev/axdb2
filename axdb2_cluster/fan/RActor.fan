

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
    
    Obj[] args := msg
    method := args[0]
    
    //echo("RActor receive: $msg")
    
    Obj? res
    if (method == "execute") {
      //echo(args)
      Str command := args[1]
      Int type := args[2]
      res = node.val.execute(command, type)
    }
    else if (method == "appendEntries") {
      AppendEntriesReq req := args[1]
      res = node.val.onAppendEntries(req)
    }
    else if (method == "requestVote") {
      RequestVoteReq req := args[1]
      res = node.val.onRequestVote(req)
    }
    else if (method == "checkTimeout") {
      node.val.checkTimeout
      this.sendLater(200ms, ["checkTimeout"])
      return null
    }

    return Unsafe(res)
  }
}