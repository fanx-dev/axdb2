

using concurrent

const class RActor : Actor {
  const ActorWorker worker := ActorWorker.fromActor(this)

  const Unsafe<RNode> node

  new make(File dir, Str name, Uri id) : super.make() {
    node = Unsafe(Node(dir, name, id))
  }

  protected override Obj? receive(Obj? msg) {
    if (worker.onReceive(msg)) return null
    
    Obj[] args := msg
    method := args[0]
    if (method == "execute") {
      Array<Int8> command := args[1]
      Int type := args[2]
      node.execute(command, type)
    }
    else if (method == "appendEntries") {
      AppendEntriesReq req := args[1]
      node.val.onAppendEntries(req)
    }
    else if (method == "requestVote") {
      RequestVoteReq req := args[1]
      node.val.onRequestVote(req)
    }

    return null
  }
}