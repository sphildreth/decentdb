import unittest
import ../../src/vfs/types
import ../../src/vfs/mem_vfs
import ../../src/errors

suite "MemVfs Tests":
  test "open/create vs open without create":
    let vfs = newMemVfs()
    
    # Should fail if not exists and create=false
    let res1 = vfs.open("test.db", fmReadWrite, false)
    check(not res1.ok)
    check(res1.err.code == ERR_IO)
    
    # Should succeed with create=true
    let res2 = vfs.open("test.db", fmReadWrite, true)
    check(res2.ok)
    let file = res2.value
    
    # Now should succeed with create=false
    let res3 = vfs.open("test.db", fmReadWrite, false)
    check(res3.ok)
    
    discard vfs.close(file)
    discard vfs.close(res3.value)

  test "write/read roundtrip (bytes + string variants)":
    let vfs = newMemVfs()
    let file = vfs.open("test.db", fmReadWrite, true).value
    
    # bytes
    let dataBytes = @[1'u8, 2, 3, 4]
    check(vfs.write(file, 0, dataBytes).value == 4)
    
    var readBytes = newSeq[byte](4)
    check(vfs.read(file, 0, readBytes).value == 4)
    check(readBytes == dataBytes)
    
    # string
    let dataStr = "hello"
    check(vfs.writeStr(file, 4, dataStr).value == 5)
    
    var readStr = newString(5)
    check(vfs.readStr(file, 4, readStr).value == 5)
    check(readStr == dataStr)
    
    discard vfs.close(file)
    
  test "write at offset past EOF (zero-fill)":
    let vfs = newMemVfs()
    let file = vfs.open("test.db", fmReadWrite, true).value
    
    let dataBytes = @[1'u8]
    # Write at offset 5
    check(vfs.write(file, 5, dataBytes).value == 1)
    
    # Size should be 6
    check(vfs.getFileSize("test.db").value == 6)
    
    var readBytes = newSeq[byte](6)
    check(vfs.read(file, 0, readBytes).value == 6)
    
    check(readBytes[0] == 0)
    check(readBytes[4] == 0)
    check(readBytes[5] == 1)
    
    discard vfs.close(file)

  test "truncate smaller and larger":
    let vfs = newMemVfs()
    let file = vfs.open("test.db", fmReadWrite, true).value
    
    let dataStr = "1234567890"
    discard vfs.writeStr(file, 0, dataStr)
    
    # Truncate smaller
    check(vfs.truncate(file, 5).ok)
    check(vfs.getFileSize("test.db").value == 5)
    
    # Truncate larger
    check(vfs.truncate(file, 10).ok)
    check(vfs.getFileSize("test.db").value == 10)
    
    var readStr = newString(10)
    discard vfs.readStr(file, 0, readStr)
    check(readStr[0..4] == "12345")
    check(readStr[5] == '\0') # Zero filled
    
    discard vfs.close(file)

  test "getFileSize accuracy":
    let vfs = newMemVfs()
    let file = vfs.open("test.db", fmReadWrite, true).value
    
    check(vfs.getFileSize("test.db").value == 0)
    
    discard vfs.writeStr(file, 0, "hello")
    check(vfs.getFileSize("test.db").value == 5)
    
    discard vfs.close(file)
    # Even after close, file is deleted from memVfs! Wait, let's see.
    # Ah! I designed `close` to remove the file.
    
  test "fileExists/removeFile behavior":
    let vfs = newMemVfs()
    check(not vfs.fileExists("test.db"))
    
    let file = vfs.open("test.db", fmReadWrite, true).value
    check(vfs.fileExists("test.db"))
    
    check(vfs.removeFile("test.db").ok)
    check(not vfs.fileExists("test.db"))
    
    check(not vfs.removeFile("test.db").ok) # error if missing
    
    discard vfs.close(file) # Since removed, close shouldn't crash

  test "mmap methods return not supported":
    let vfs = newMemVfs()
    let file = vfs.open("test.db", fmReadWrite, true).value
    
    check(not vfs.supportsMmap())
    
    let mapRes = vfs.mapWritable(file, 1024)
    check(not mapRes.ok)
    check(mapRes.err.code == ERR_INTERNAL)
    
    let unmapRes = vfs.unmap(MmapRegion())
    check(not unmapRes.ok)
    check(unmapRes.err.code == ERR_INTERNAL)
    
    discard vfs.close(file)
