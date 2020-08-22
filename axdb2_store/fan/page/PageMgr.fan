//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//

**
** PageMgr
**
class PageMgr
{
  ** byte num of a page
  Int pageSize := 1024 * 8 { private set }
  private Int pageCount := 0
  private Int version := 0
  [Str:Str] meta := [:]
  private Int[] freePages := [,]
  
  private Int[] peddingFreePages := [,]
  
  ** file dir
  private File dir
  ** db short file name
  Str name { private set }
  
  static const Log log := Log.get("axdb2_store")
  
  ** part file num to Buf map
  private [Int:Buf] partMap := [:]
  ** page num of a part
  private Int pagePerPart := 100 * 1024
  
  private Bool isReadOnly
  
  ** page LRU cache
  //private LruCache cache
  
  new make(File dir, Str name, Bool isReadOnly = false) {
    this.name = name
    this.dir = dir
    this.isReadOnly = isReadOnly
    //this.cache = cache
    file := (dir+`${name}.meta`)
    if (file.exists) {
        reload
    }
    else {
        file2 := (dir+`${name}.meta.tmp`)
        if (file2.exists) {
            //recover
            file2.rename("${name}.meta")
            reload
        }
        else {
            out := file.out
            writeHeader(out)
            out.close
        }
    }
  }
  
  Void reload() {
    file := (dir+`${name}.meta`)
    in := file.in
    readHeader(in)
    in.close
  }
  
  Void dump() {
    echo("pageSize:$pageSize, pageCount:$pageCount, freePages:$freePages")
  }
  
  private Void readHeader(InStream in) {
    version = in.readS4
    pageSize = in.readS4
    pageCount = in.readS8
    
    meta.clear
    metaNum := in.readS4
    metaNum.times {
        k := in.readUtf
        v := in.readUtf
        meta[k] = v 
    }
    
    freePageNum := in.readS8
    freePages.clear
    freePages.capacity = freePageNum
    freePageNum.times {
        freePages.add(in.readS8)
    }
  }
  
  private Void writeHeader(OutStream out) {
    out.writeI4(version)
    out.writeI4(pageSize)
    out.writeI8(pageCount)
    out.writeI4(meta.size)
    meta.each |v, k| {
        out.writeUtf(k)
        out.writeUtf(v)
    }
    
    out.writeI8(freePages.size)
    
    freePages.each {
        out.writeI8(it)
    }
  }
  
  virtual Void close() {
    flush
    partMap.each |v, k| {
      v.close
    }
  }
  
  virtual Void flush() {
    partMap.each |v, k| {
      v.sync
    }
    
    freePages.addAll(peddingFreePages)
    peddingFreePages.clear
    
    file := (dir+`${name}.meta.tmp`)
    out := file.out
    writeHeader(out)
    out.close
    
    
    oldFile := (dir+`${name}.meta`)
    oldFile.delete
    file.rename("${name}.meta")
  }
  
  virtual Int createPageId() {
    if (freePages.size > 0) {
        id := freePages.pop
        return id
    }
  
    page := pageCount
    ++pageCount
    return page
  }
  
  Page createPage() {
    Page(pageSize, createPageId)
  }
  
  virtual Void savePage(Page page) {
    log.debug("savePage:$page.id")
    pageId := page.id
    out := storeOut(pageId)
    page.write(out)
  }
  
  ** get page by page ID
  virtual Page getPage(Int pageId) {
    //Page? page = cache.get(pageId)
    //if (page != null) return page
    page := loadPage(pageId)
    //cache.set(pageId, page)
    return page
  }

  protected virtual Page loadPage(Int pageId) {
    if (pageId >= pageCount) {
      throw Err("pageId error: $pageId >= $pageCount")
    }
    log.debug("loadPage $pageId")
    in := storeIn(pageId)

    page := Page(this.pageSize, pageId)
    if (in != null) {
        page.read(in)
    }
    return page
  }
  
  virtual Void freePage(Int pageId) {
    peddingFreePages.add(pageId)
  }
  
  private File getPartFile(Int fileId) {
    dir + `$name-${fileId}.dat`
  }
  
  private Buf? getBuf(Int pageId, Bool readMode) {
    fileId := pageId / pagePerPart
    if (pageId == Page.invalidId) {
      throw Err("invalid pageId")
    }
    buf := partMap[fileId]
    if (buf == null) {
      file := getPartFile(fileId)
      buf = file.open(isReadOnly?"r":"rw")
      partMap[fileId] = buf
    }

    //seek buf
    pageId = pageId % pagePerPart
    pos := pageId * pageSize

    if (readMode && pos >= buf.size) {
      //echo("pageId out: $pos >= $buf.size")
      return null
    }
    
    if (pos > buf.size) buf.size = pos
    //echo("getBuf:$buf, $pos")
    buf.seek(pos)
    return buf
  }

  protected virtual InStream? storeIn(Int pageId) {
    getBuf(pageId, true)?.in
  }

  protected virtual OutStream storeOut(Int pageId) {
    getBuf(pageId, false).out
  }
}

