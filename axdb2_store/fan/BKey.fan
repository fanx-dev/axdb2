//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//

class BKey {
    Array<Int8> key
    Int hashKey
    Array<Int8>? value
    
    new make(Array<Int8> key) {
        this.key = key
        
        hash := 0
        for (i:=0; i<key.size; ++i) {
            hash += key[i]
        }
        this.hashKey = hash
    }
    
    Bool byteEquals(Array<Int8> other) {
        if (key.size != other.size) return false
        for (i:=0; i<key.size; ++i) {
            if (key[i] != other[i]) return false
        }
        return true
    }
    
    override Str toStr() {
        "$hashKey:"+Str.fromUtf8(key)
    }
}