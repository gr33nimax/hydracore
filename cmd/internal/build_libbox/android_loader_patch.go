package main

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	seqClassPath       = "go/Seq.class"
	seqSourcePath      = "go/Seq.java"
	systemClassName    = "java/lang/System"
	hydraLoaderClass   = "go/HydraNativeLoader"
	loadLibraryName    = "loadLibrary"
	loadLibraryType    = "(Ljava/lang/String;)V"
	legacyLoaderSource = "System.loadLibrary(\"box\");"
	hydraLoaderSource  = "HydraNativeLoader.loadLibrary(\"box\");"
)

func patchAndroidLoader(aarPath string) error {
	classesSeen := false
	seqClassSeen := false
	if err := rewriteZip(aarPath, func(name string, value []byte) ([]byte, error) {
		if name != "classes.jar" {
			return value, nil
		}
		classesSeen = true
		return rewriteZipBytes(value, func(nestedName string, nestedValue []byte) ([]byte, error) {
			if nestedName != seqClassPath {
				return nestedValue, nil
			}
			seqClassSeen = true
			return patchSeqClass(nestedValue)
		})
	}); err != nil {
		return fmt.Errorf("patch generated HydraNativeLoader call: %w", err)
	}
	if !classesSeen || !seqClassSeen {
		return fmt.Errorf("generated AAR does not contain %s", seqClassPath)
	}

	sourcesPath := strings.TrimSuffix(aarPath, filepath.Ext(aarPath)) + "-sources.jar"
	if _, err := os.Stat(sourcesPath); err == nil {
		seqSourceSeen := false
		if err = rewriteZip(sourcesPath, func(name string, value []byte) ([]byte, error) {
			if name != seqSourcePath {
				return value, nil
			}
			seqSourceSeen = true
			source := string(value)
			if strings.Contains(source, hydraLoaderSource) {
				return value, nil
			}
			if !strings.Contains(source, legacyLoaderSource) {
				return nil, fmt.Errorf("generated Seq.java has no recognized loader call")
			}
			return []byte(strings.Replace(source, legacyLoaderSource, hydraLoaderSource, 1)), nil
		}); err != nil {
			return fmt.Errorf("patch generated source archive: %w", err)
		}
		if !seqSourceSeen {
			return fmt.Errorf("generated source archive does not contain %s", seqSourcePath)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

type constantPoolEntry struct {
	tag              byte
	raw              []byte
	utf8             string
	classIndex       uint16
	nameAndTypeIndex uint16
	nameIndex        uint16
	descriptorIndex  uint16
}

func patchSeqClass(class []byte) ([]byte, error) {
	if len(class) < 10 || binary.BigEndian.Uint32(class[:4]) != 0xcafebabe {
		return nil, fmt.Errorf("invalid Seq.class")
	}
	count := int(binary.BigEndian.Uint16(class[8:10]))
	entries := make([]constantPoolEntry, count)
	offset := 10
	for index := 1; index < count; index++ {
		entryIndex := index
		wideEntry := false
		if offset >= len(class) {
			return nil, fmt.Errorf("truncated constant pool")
		}
		start := offset
		tag := class[offset]
		offset++
		entry := constantPoolEntry{tag: tag}
		switch tag {
		case 1:
			if offset+2 > len(class) {
				return nil, fmt.Errorf("truncated UTF-8 constant")
			}
			length := int(binary.BigEndian.Uint16(class[offset : offset+2]))
			offset += 2
			if offset+length > len(class) {
				return nil, fmt.Errorf("truncated UTF-8 value")
			}
			entry.utf8 = string(class[offset : offset+length])
			offset += length
		case 3, 4:
			offset += 4
		case 5, 6:
			offset += 8
			wideEntry = true
		case 7, 8, 16, 19, 20:
			if offset+2 <= len(class) {
				entry.nameIndex = binary.BigEndian.Uint16(class[offset : offset+2])
			}
			offset += 2
		case 9, 10, 11, 12, 17, 18:
			if offset+4 <= len(class) {
				entry.classIndex = binary.BigEndian.Uint16(class[offset : offset+2])
				entry.nameAndTypeIndex = binary.BigEndian.Uint16(class[offset+2 : offset+4])
				if tag == 12 {
					entry.nameIndex = entry.classIndex
					entry.descriptorIndex = entry.nameAndTypeIndex
				}
			}
			offset += 4
		case 15:
			offset += 3
		default:
			return nil, fmt.Errorf("unsupported constant pool tag %d", tag)
		}
		if offset > len(class) {
			return nil, fmt.Errorf("truncated constant pool entry")
		}
		entry.raw = append([]byte(nil), class[start:offset]...)
		entries[entryIndex] = entry
		if wideEntry {
			index++
		}
	}
	poolEnd := offset

	utf8At := func(index uint16) string {
		if int(index) >= len(entries) {
			return ""
		}
		return entries[index].utf8
	}
	classNameAt := func(index uint16) string {
		if int(index) >= len(entries) || entries[index].tag != 7 {
			return ""
		}
		return utf8At(entries[index].nameIndex)
	}

	methodIndex := 0
	for index := 1; index < len(entries); index++ {
		entry := entries[index]
		if entry.tag != 10 || classNameAt(entry.classIndex) != systemClassName {
			continue
		}
		if int(entry.nameAndTypeIndex) >= len(entries) {
			continue
		}
		nameType := entries[entry.nameAndTypeIndex]
		if nameType.tag == 12 &&
			utf8At(nameType.nameIndex) == loadLibraryName &&
			utf8At(nameType.descriptorIndex) == loadLibraryType {
			methodIndex = index
			break
		}
	}
	if methodIndex == 0 {
		for index := 1; index < len(entries); index++ {
			entry := entries[index]
			if entry.tag == 10 && classNameAt(entry.classIndex) == hydraLoaderClass {
				return class, nil
			}
		}
		return nil, fmt.Errorf("System.loadLibrary method reference not found")
	}
	if count > 65533 {
		return nil, fmt.Errorf("constant pool is full")
	}
	loaderUtf8Index := uint16(count)
	loaderClassIndex := uint16(count + 1)

	var result bytes.Buffer
	result.Write(class[:8])
	_ = binary.Write(&result, binary.BigEndian, uint16(count+2))
	for index := 1; index < count; index++ {
		entry := entries[index]
		if len(entry.raw) == 0 {
			continue
		}
		if index == methodIndex {
			patched := append([]byte(nil), entry.raw...)
			binary.BigEndian.PutUint16(patched[1:3], loaderClassIndex)
			result.Write(patched)
		} else {
			result.Write(entry.raw)
		}
	}
	result.WriteByte(1)
	_ = binary.Write(&result, binary.BigEndian, uint16(len(hydraLoaderClass)))
	result.WriteString(hydraLoaderClass)
	result.WriteByte(7)
	_ = binary.Write(&result, binary.BigEndian, loaderUtf8Index)
	result.Write(class[poolEnd:])
	return result.Bytes(), nil
}

func rewriteZip(path string, transform func(string, []byte) ([]byte, error)) error {
	input, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	output, err := rewriteZipBytes(input, transform)
	if err != nil {
		return err
	}
	temporary := path + ".hydrabox.tmp"
	if err = os.WriteFile(temporary, output, 0o644); err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func rewriteZipBytes(input []byte, transform func(string, []byte) ([]byte, error)) ([]byte, error) {
	reader, err := zip.NewReader(bytes.NewReader(input), int64(len(input)))
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	found := false
	for _, file := range reader.File {
		stream, openErr := file.Open()
		if openErr != nil {
			return nil, openErr
		}
		value, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		transformed, transformErr := transform(file.Name, value)
		if transformErr != nil {
			return nil, transformErr
		}
		if !bytes.Equal(value, transformed) {
			found = true
		}
		header := file.FileHeader
		header.CRC32 = 0
		header.CompressedSize = 0
		header.CompressedSize64 = 0
		header.UncompressedSize = 0
		header.UncompressedSize64 = 0
		destination, createErr := writer.CreateHeader(&header)
		if createErr != nil {
			return nil, createErr
		}
		if _, createErr = destination.Write(transformed); createErr != nil {
			return nil, createErr
		}
	}
	if !found {
		// Already-patched archives are valid and remain reproducible.
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
