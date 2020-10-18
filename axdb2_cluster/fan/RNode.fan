// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

using concurrent

enum class Role {
  leader, follower, candidate
}

**
** RNode
**
class RNode
{
    ** 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private Int currentTerm

    ** 在当前获得选票的候选人的 Id
    private Uri? votedFor
    
    ** 元数据保存路径
    private File metaFile
    
    ** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
    private Logs logs

  
    private Role role := Role.follower { private set }
    
    ** 已知的最大的已经被提交的日志条目的索引值
    private Int commitIndex
    
    ** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private Int lastApplied
    
    ** 本地状态机
    StateMachine stateMachine { private set }
    //Lock stateMachinelock := Lock()
    
    //Lock lock := Lock()
    static const Log log := Log.get("axdb2_cluster")
    
    private Int receiveHeartbeatTime
    private Int lastSendHeartbeatTime
    private Int electionStartTime
    
    private Int timeout := 200

    ** local server id
    private Uri id
    private RConfiguration configuration
    private Uri? leaderId
    
    private Int installingSnapCount := 0
    private Int lastIncludedIndex := -1
    private Int lastIncludedTerm := -1
    
    private File dir
    private Str name
    
    new make(File dir, Str name, Uri id) {
        if (!dir.isDir) {
            throw ArgErr("$dir is not dir")
        }
        if (!dir.exists) {
            dir.create
        }
        
        this.dir = dir
        this.name = name
        
        stateMachine = StoreStateMachine(dir, name)
        configuration = RConfiguration(id, dir, name)
        metaFile = dir + `${name}-rf.meta`
        if (metaFile.exists) loadMeta(metaFile)
        else saveMeta
        
        snapshotPoint := stateMachine.snapshotPoint
        if (snapshotPoint > 0) {
            commitIndex = snapshotPoint
            lastApplied = snapshotPoint
        }
        
        this.id = id
        logs = Logs(dir, name)
    }

    override Str toStr() {
        return "role:$role, currentTerm:$currentTerm, lastLog:$logs.lastIndex, commitIndex:$commitIndex, leaderId:$leaderId"
    }
    
    Str onDump() {
        str := toStr + ", ${configuration.members}"
        echo("RNode: $str")
        logs.dump
        stateMachine.dump
        return str
    }
    
    private Void saveMeta(File file := metaFile) {
        path := file.pathStr
        tempFile := File.fromPath(path+".tmp", false)
        out := tempFile.out
        out.writeI8(0)
        out.writeI8(currentTerm)
        out.writeUtf(votedFor == null ? "" : votedFor.toStr)
        //out.writeI8(snapshotPoint)
        
        out.sync
        out.close
        file.delete
        tempFile.rename(file.name)
    }

    private Void loadMeta(File file) {
        in := file.in
        in.readS8
        currentTerm = in.readS8
        votedFor = in.readUtf.toUri
        //snapshotPoint = in.readS8

//        commitIndex = snapshotPoint
//        lastApplied = snapshotPoint

        in.close
    }

    ** 执行客户端命令
    async Bool onExecute(Str command, Int type, Bool sync) {
        if (role != Role.leader) {
            return false
        }
        if (log.isDebug) log.debug("execute: $command, type:$type")
        
        logEntry := LogEntry(currentTerm, logs.lastIndex+1, command, type)
        logs.add(logEntry)
        //echo(logs.lastIndex)
        
        lastSendHeartbeatTime = TimePoint.nowMillis
        configuration.eachPeer(id) |peer|{
            replicateTo(peer, false)
        }

        if (sync) {
            await Async.sleep(5ms)
            logs.sync

            while (true) {
                advanceCommitIndex
                if (commitIndex >= logEntry.index) {
                    break
                }
                await Async.sleep(5ms)
            }
        }
        else {
            logs.flush
        }
        
        if (logEntry.type == 1 || logEntry.type == 2) {
            configuration.apply(logEntry, logs.lastIndex)
        }
        
        return true
    }
    
    private async Bool sendInstallSnapshot(Peer peer) {
        ++installingSnapCount
        peer.installingSnap = true
        success := false
        try {
            offset := 0
            while (true) {
                InstallSnapshotReq? req := stateMachine.snapshotChunk(offset)
                if (req == null) break
                data := req.data
                offset += data.size
                done := req.done
                
                lastIncludedIndex := 0
                lastIncludedTerm := 0
                if (done) {
                    lastIncludedIndex = stateMachine.snapshotPoint
                    log := logs.get(lastIncludedIndex)
                    lastIncludedTerm = log.term
                }
                req {
                    it.term = currentTerm
                    it.leaderId = this.leaderId
                    it.lastIncludedIndex = lastIncludedIndex
                    it.lastIncludedTerm = lastIncludedTerm
                }
                res := await peer.sendInstallSnapshot(req)
                if (res == null) break
                if (res.term != req.term) break
                if (done) {
                    success = true
                    peer.nextIndex = lastIncludedIndex+1
                    peer.matchIndex = lastIncludedIndex
                    break
                }
            }
        }
        catch {
        }
        --installingSnapCount
        peer.installingSnap = false
        return success
    }
    
    private async Bool replicateTo(Peer peer, Bool heartbeat) {
        if (peer.installingSnap) return false
        
        nextIndex := peer.nextIndex
        LogEntry[]? logList
        
        //if (!heartbeat) echo("replicateTo1: lastIndex:$logs.lastIndex, nextIndex:$nextIndex, $peer")
      
        logLastIndex := logs.lastIndex
        if (nextIndex < logs.minIndex) {
            echo("InstallSnapshot:$peer, minIndex:logs.minIndex")
            //logList = [,]
            sendInstallSnapshot(peer)
            return false
        }
        else if (logLastIndex >= nextIndex) {
            logList = logs.getFrom(nextIndex)
            //echo("replicateTo2: lastIndex:$logs.lastIndex, nextIndex:$nextIndex, $peer, $logList")
        }
        else {
            if (heartbeat) {
                logList = [,]
            }
            else {
                return false
            }
        }
        
        prevLogEntry := logs.get(nextIndex-1)
        ae := AppendEntriesReq {
            it.term = currentTerm
            it.leaderId = id
            it.prevLogIndex = prevLogEntry == null ? -1 : prevLogEntry.index
            it.prevLogTerm = prevLogEntry == null ? -1 : prevLogEntry.term
            it.entries = logList
            it.leaderCommit = commitIndex
        }
        
        res := await peer.sendAppendEntries(ae)
        if (res == null) return false
        
        if (res.success) {
            if (nextIndex <= logLastIndex) {
                peer.nextIndex = nextIndex + 1
                peer.matchIndex = nextIndex
            }
            else {
                peer.matchIndex = logLastIndex
            }
        }
        else {
            if (nextIndex - 1 > 0) {
                peer.nextIndex = nextIndex - 1
            }
        }
        if (logList.size > 0) echo("end send $peer, $res")
        return false
    }
    
    private Void advanceCommitIndex() {
        //echo("advanceCommitIndex: commitIndex:$commitIndex, logs:$logs.lastIndex")
        for (i:=commitIndex+1; i<=logs.lastIndex; ++i) {
            logEntry := logs.get(i)
            //if (logEntry.term == currentTerm) {
                count := 1
                configuration.eachPeer(id) |peer|{
                    if (peer.matchIndex >= logEntry.index) ++count
                    //echo("advanceCommitIndex: $peer")
                }
                //echo("advanceCommitIndex: $count")
                if (count > configuration.members.size/2) {
                    if (log.isDebug) log.debug("commitIndex to: $i")
                    commitIndex = i
                }
            //}
        }
    }
    
    
    private Void checkApplayLog() {
        if (role == Role.leader) advanceCommitIndex
        while (commitIndex > lastApplied) {
            ++lastApplied
            logEntry := logs.get(lastApplied)
            
            if (logEntry != null && logEntry.type == 0) {
                if (log.isDebug) log.debug("applay log: $logEntry")
                stateMachine.apply(logEntry.command, logEntry.index)
                takeSnapshot
            }
        }
    }
    
    internal AppendEntriesRes onAppendEntries(AppendEntriesReq req) {
        receiveHeartbeatTime = TimePoint.nowMillis
        if (req.term >= currentTerm) {
            stepDown(req.term)
        }
        else {
            //echo("term error: $req.term, $currentTerm")
            return AppendEntriesRes(currentTerm, false)
        }
        
        if (req.prevLogIndex != -1) {
            logEntry := logs.get(req.prevLogIndex)
            if (logEntry == null || logEntry.term != req.prevLogTerm) {
                if (req.prevLogIndex == lastIncludedIndex && lastIncludedTerm == req.prevLogTerm) {
                    lastIncludedIndex = -1
                    lastIncludedTerm = -1
                }
                else {
                    if (log.isDebug) log.debug("prevLogIndex error2: $req.prevLogIndex, logEntry")
                    return AppendEntriesRes(currentTerm, false)
                }
            }
            //echo("=======Logs addAndRemove begin")
            //logs.dump
        }
        
        if (req.entries.size > 0) {
            logs.addAndRemove(req.entries)
            logs.sync
        }
        
        if (req.leaderCommit > commitIndex) {
            lastEntry := logs.last
            if (lastEntry != null) {
                commitIndex = req.leaderCommit.min(lastEntry.index)
            }
        }
        
        checkApplayLog
        this.leaderId = req.leaderId
        
        req.entries.each |log| {
            if (log.type == 1 || log.type == 2) {
                if (!configuration.inGroup(leaderId)) {
                    configuration.addPeer(leaderId)
                }
                configuration.apply(log, logs.lastIndex)
            }
        }
        
        return AppendEntriesRes(currentTerm, true)
    }
    
    InstallSnapshotRes onInstallSnapshot(InstallSnapshotReq req) {
        receiveHeartbeatTime = TimePoint.nowMillis
        echo("onInstallSnapshot $req")
        if (req.term < currentTerm) {
            echo("onInstallSnapshot fail: term:$req.term, $currentTerm")
            return InstallSnapshotRes(currentTerm)
        }
        leaderId = req.leaderId
        if (req.offset == 0) {
            temDir := dir + `$name-snapshot/`
            if (temDir.exists) {
                temDir.delete
            }
            temDir.create
        }
        
        file := dir + `$name-snapshot/$name-${req.fileId}.dat`
        if (req.fileId == -1) {
            file = dir + `$name-snapshot/${name}.meta`
        }
        buf := file.open
        buf.seek(req.fileOffset)
        buf.out.writeBytes(req.data)
        buf.close
        
        if (req.done) {
            lastIncludedIndex = req.lastIncludedIndex
            lastIncludedTerm = req.lastIncludedTerm
            temDir := dir + `$name-snapshot/`
            temDir.listFiles.each | f| {
                f.copyInto(dir, ["overwrite":true])
            }
            stateMachine = StoreStateMachine(dir, name)
            
            //remove log file
            dir.listFiles.each | f| {
                if (f.name.startsWith(name+"-log.")) {
                    f.delete
                }
            }
            logs = Logs(dir, name)
            
            snapshotPoint := stateMachine.snapshotPoint
            if (snapshotPoint > 0) {
                commitIndex = snapshotPoint
                lastApplied = snapshotPoint
            }
            
            saveMeta
        }
        
        return InstallSnapshotRes(currentTerm)
    }
    
    private Void sendHeartbeat() {
        lastSendHeartbeatTime = TimePoint.nowMillis
        configuration.eachPeer(id) |peer|{
            replicateTo(peer, true)
        }
    }
    
    private Void stepDown(Int term) {
        currentTerm = term
        role = Role.follower
        votedFor = null
        saveMeta
    }
    
    private Void becomeLeader() {
        role = Role.leader
        leaderId = id
        if (log.isDebug) log.debug("becomeLeader:$this")
        
        sendHeartbeat
        //execute("becomeLeader", 3)

        lastIndex := logs.lastIndex
        configuration.eachPeer(id) |peer|{
            peer.nextIndex = lastIndex + 1
            peer.matchIndex = 0
        }
    }
    
    
    internal Void checkTimeout() {
        //echo("checkTimeout:$this")
        now := TimePoint.nowMillis
        // 心跳超时
        if (role == Role.follower) {
            if (now - receiveHeartbeatTime > timeout || votedFor != null) {
                role = Role.candidate
                startElection
            }
        }
        // 如果选举过程超时，再次发起一轮选举
        else if (role == Role.candidate) {
            if (now - electionStartTime > Int.random(timeout..2000)) {
                startElection
            }
        }
        else if (role == Role.leader) {
            if (now - lastSendHeartbeatTime > timeout/2) {
                sendHeartbeat
            }
        }
        
        checkApplayLog
    }
    
    
    
    internal RequestVoteRes onRequestVote(RequestVoteReq req) {
        now := TimePoint.nowMillis
        if (role == Role.follower) {
            //没有超时，当前领导人存在
            if (now - receiveHeartbeatTime < timeout) {
                if (log.isDebug) log.debug("onRequestVote: leader no timeout")
                return RequestVoteRes(currentTerm, false)
            }
        }
        
        if (req.term > currentTerm) {
            stepDown(req.term)
        }
        
        if (req.term < currentTerm) {
            if (log.isDebug) log.debug("onRequestVote: currentTerm error: req.term:$req.term, currentTerm:$currentTerm")
            return RequestVoteRes(currentTerm, false)
        }
        
        if (votedFor == null || votedFor == req.candidateId) {
            logEntry := logs.lastIndex
            if (req.lastLogIndex >= logEntry) {
                votedFor = req.candidateId
                saveMeta
                return RequestVoteRes(currentTerm, true)
            }
        }
        
        if (log.isDebug) log.debug("onRequestVote: already votedFor: $votedFor")
        return RequestVoteRes(currentTerm, false)
    }
    
    ** 开始选举
    private async Void startElection() {
        if (log.isDebug) log.debug("startElection:$this")
        ++currentTerm
        votedFor = id
        electionStartTime = TimePoint.nowMillis
        logEntry := logs.last
        req := RequestVoteReq {
            it.term = currentTerm
            it.candidateId = id
            it.lastLogIndex = logEntry == null ? -1 : logEntry.index
            it.lastLogTerm = logEntry == null ? -1 : logEntry.term
        }
        
        list := Promise<RequestVoteRes>[,]
        configuration.eachPeer(id) |peer|{
            r := peer.sendRequestVote(req)
            list.add(r)
        }
        
        count := 1
        voteGranted := false
        for (i:=0; i<list.size; ++i) {
            RequestVoteRes? res := await list[i]
            if (res == null) continue
            if (res.voteGranted) {
                ++count
                if (count > configuration.members.size/2) {
                    voteGranted = true
                    break
                }
            }
        }
        if (!voteGranted) {
            if (count > configuration.members.size/2) {
                voteGranted = true
            }
        }
        if (log.isDebug) log.debug("voteGranted:$voteGranted, count:$count")
        if (voteGranted) {
            becomeLeader
        }
        saveMeta
    }
    
    private Void takeSnapshot() {
        if (installingSnapCount > 0) return
        if (stateMachine.isBusy) return
        snapshotPoint := stateMachine.snapshotPoint
        snapshotLimit := this.typeof.pod.config("snapshotLimit", "900000").toInt
        
        if (lastApplied - snapshotPoint > snapshotLimit) {
            if (log.isDebug) log.debug("takeSnapshot, lastApplied:$lastApplied, snapshotPoint:$snapshotPoint")
            //logs.truncBefore(snapshotPoint-snapshotLimit)
            logs.truncBefore(snapshotPoint)
            stateMachine.saveSnapshot
        }
    }
}
