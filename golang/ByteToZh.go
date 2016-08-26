package stringUtils

import (
	"unicode/utf16"
)

func Bytes2runes(byteArray []byte) []rune {
	//make a uint16 array
	u16Array := make([]uint16, len(byteArray)/2)
	for i := 0; i < len(u16Array); i++ {
		u16Array[i] = uint16(byteArray[i*2])<<8 | uint16(byteArray[i*2+1])
	}

	return utf16.Decode(u16Array)
}
