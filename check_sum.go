/**
 * @note
 * 计算强校验和弱校验值
 *
 * @author	songtianming
 * @date 	2020-07-07
 */
package rsync

import (
	"encoding/base64"
	"golang.org/x/crypto/md4"
)

func calWeakSum(buf []byte) (uint16, uint16) {
	var a, b, c int
	i, l := 0, len(buf)
	for l > 0 {
		c = int(buf[i])
		a = a + c
		b = b + (l * c)
		i, l = i+1, l-1
	}
	return uint16(a), uint16(b)
}

func joinWeakSum(a, b uint16) int {
	return int(a)<<16 + int(b)
}

func calStrongSum(data []byte) []byte {
	ctx := md4.New()
	ctx.Write(data)
	src := ctx.Sum(nil)
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return dst
}

func CompareBytes(b1 []byte, b2 []byte) bool {
	if l1, l2 := len(b1), len(b2); l1 != l2 {
		return false
	} else {
		for i := 0; i < l1; i++ {
			if b1[i] != b2[i] { //逐个比较
				return false
			}
		}
	}
	return true
}

type RCheckSum struct {
	Order int    //block order
	A     uint16 //weak sum a
	B     uint16 //weak sum b
	SS    []byte //strong sum
}

func calCheckSumByBlockLen(buf []byte, blockLen int) (checkSum []RCheckSum) {
	for order, start, end, l := 0, 0, blockLen, len(buf); start < l; order, start, end = order+1, end, end+blockLen {
		tmpb := fillBlankByBlockLen(buf[start:], blockLen)
		c := RCheckSum{Order: order, SS: calStrongSum(tmpb)}
		c.A, c.B = calWeakSum(tmpb)
		checkSum = append(checkSum, c)
	}
	return checkSum
}

func fillBlankByBlockLen(buf []byte, blockLen int) []byte {
	if len(buf) < blockLen {
		return append(buf, make([]byte, blockLen-len(buf))...)
	} else {
		return buf[:blockLen]
	}
}

func cutByBlockLen(buf []byte, blockLen int) []byte {
	if len(buf) < blockLen {
		return buf
	} else {
		return buf[:blockLen]
	}
}
