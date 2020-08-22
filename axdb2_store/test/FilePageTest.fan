
class FilePageTest : Test {
  File path := File(`data/`)
  Str name := "page"
  
  override Void setup() {
    path.delete
  }

  private Void read() {
     store := PageMgr(path, name)
     store.dump

     page := store.getPage(0)
     in := MemBuf.makeBuf(page.content)
     str := in.readUtf
     verifyEq(str, "Hello")

     page2 := store.getPage(1)
     in2 := MemBuf.makeBuf(page2.content)
     str2 := in2.readUtf
     verifyEq(str2, "World")

     store.close
  }

  private Void write() {
     store := PageMgr(path, name)
     page := store.createPage()
     out := MemBuf.makeBuf(page.content)
     out.writeUtf("Hello")
     out.close
     store.savePage(page)

     page2 := store.createPage()
     out2 := MemBuf.makeBuf(page2.content)
     out2.writeUtf("World")
     out2.close
     store.savePage(page2)

     store.close
     store.dump
     echo("--")
  }

  Void test() {
    write
    read
  }

  Void testEmpty() { 
    store := PageMgr(path, name)
    page := store.createPage()
    page2 := store.getPage(0)

    verifyEq(page.content.size, page2.content.size)
    //verifyEq(page.buf.pos, page2.buf.pos)
    verifyEq(page.id, page2.id)
  }
}