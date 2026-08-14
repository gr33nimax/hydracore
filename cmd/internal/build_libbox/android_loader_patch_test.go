package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPatchSeqClassRedirectsOnlyLoadLibraryOwner(t *testing.T) {
	var class bytes.Buffer
	_ = binary.Write(&class, binary.BigEndian, uint32(0xcafebabe))
	_ = binary.Write(&class, binary.BigEndian, uint16(0))
	_ = binary.Write(&class, binary.BigEndian, uint16(52))
	_ = binary.Write(&class, binary.BigEndian, uint16(8))
	writeUTF8 := func(value string) {
		class.WriteByte(1)
		_ = binary.Write(&class, binary.BigEndian, uint16(len(value)))
		class.WriteString(value)
	}
	writeUTF8(systemClassName) // 1
	class.Write([]byte{7, 0, 1})
	writeUTF8(loadLibraryName) // 3
	writeUTF8(loadLibraryType) // 4
	class.Write([]byte{12, 0, 3, 0, 4})
	class.Write([]byte{10, 0, 2, 0, 5})
	writeUTF8("sentinel")
	class.Write([]byte{0, 0, 0, 0})

	patched, err := patchSeqClass(class.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(patched, class.Bytes()) {
		t.Fatal("Seq.class was not patched")
	}
	if !bytes.Contains(patched, []byte(hydraLoaderClass)) {
		t.Fatal("patched class does not contain HydraNativeLoader")
	}
	patchedAgain, err := patchSeqClass(patched)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(patchedAgain, patched) {
		t.Fatal("patching must be idempotent")
	}
}
