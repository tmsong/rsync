/**
 * @note
 * rsync
 *
 * @author	songtianming
 * @date 	2020-07-13
 */
package rsync

import (
	"bytes"
	"sort"
	"sync"
)

type RFile struct {
	mu       sync.RWMutex
	data     []byte
	totalSum []byte
	blockLen int
	blockSum []RCheckSum
}

type RSameChunk struct { //与远端文件相同的block信息
	start int
	order int
}

type RDiffChunk struct { //与远端文件不同的block信息
	Order int
	Data  []byte
}

func NewRFile(blockLen int) *RFile {
	if blockLen < (1 << 6) {
		blockLen = 1 << 8 //默认block为256长度
	}
	return &RFile{blockLen: blockLen}
}

func (rf *RFile) SetBlockLen(l int) {
	if l == 0 {
		panic("invalid block length")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.blockLen != l {
		rf.blockLen = l
		if len(rf.data) > 0 {
			//需要重新计算
			rf.calBlockSum()
		}
	}
}

func (rf *RFile) calBlockSum() {
	if rf.blockLen == 0 {
		panic("invalid block length")
	}
	rf.blockSum = calCheckSumByBlockLen(rf.data, rf.blockLen)
}

/* @note
 * @param buf 需要set的data
 * @param calBlockSum 是否同时计算checksum，对于作server端的文件有用
 */
func (rf *RFile) SetData(buf []byte, calBlockSum bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.data = buf
	rf.totalSum = calStrongSum(rf.data)
	if calBlockSum {
		rf.calBlockSum()
	}
}

/* @note
 * 获取当前文件的属性及强弱校验值，用于校验。
 * @return blockLen 当前文件的分块长度
 * @return totalSum 当前文件的总体强校验值，用于预先对文件的比对
 * @return blockSum 当前文件分块的强弱校验值
 */
func (rf *RFile) GetProperties() (blockLen int, totalSum []byte, blockSum []RCheckSum) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	blockSum = make([]RCheckSum, len(rf.blockSum))
	totalSum = make([]byte, len(rf.totalSum))
	copy(blockSum, rf.blockSum)
	copy(totalSum, rf.totalSum)
	return rf.blockLen, totalSum, blockSum
}

func (rf *RFile) GetData() (buf []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	buf = make([]byte, len(rf.data))
	copy(buf, rf.data)
	return buf
}

/* @note
 * 根据给定的blockLen, 总体checksum和各个块的checksum，与本地比对
 * @return same 是否与目标文件完全一致
 * @return reset blockLen是否与传入不一致，需要重新计算
 * @return sameChunks 如果与目标文件不完全一致，则返回当前文件中与目标文件一致的文件块信息
 * @return diffChunkOrders 如果与目标文件不完全一致，则返回当前文件中与目标文件不一致的文件块编号
 */
func (rf *RFile) CheckByCheckSum(blockLen int, totalSum []byte, sums []RCheckSum) (same, reset bool, sameChunks []RSameChunk, diffOrders []int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.blockLen == 0 || blockLen == 0 {
		panic("invalid block length")
	}
	if CompareBytes(totalSum, rf.totalSum) {
		return true, false, nil, nil
	} else if rf.blockLen != blockLen { //need reset blockLen
		return false, true, nil, nil
	}
	//make map
	m := make(map[int][]RCheckSum)
	for _, s := range sums {
		weak := joinWeakSum(s.A, s.B)
		m[weak] = append(m[weak], s)
	}
	//roll sum
	rollSum := NewRollSum(blockLen)
NEXT:
	for init, begin, l := true, 0, len(rf.data); begin < l; {
		if init {
			rollSum.InitByBuf(fillBlankByBlockLen(rf.data[begin:], blockLen)) //recalculate after a block hit
			init = false
		} else {
			rollSum.Update(rf.data[begin], rf.data[begin+blockLen-1]) //roll sum
		}
		w := joinWeakSum(rollSum.a, rollSum.b)
		//if hit the weak sum
		if checkSums := m[w]; len(checkSums) > 0 {
			//check the strong sum
			strongSum := calStrongSum(fillBlankByBlockLen(rf.data[begin:], blockLen))
			for _, c := range checkSums {
				if CompareBytes(strongSum, c.SS) {
					sameChunks = append(sameChunks, RSameChunk{
						start: begin,
						order: c.Order,
					})
					begin += blockLen
					init = true
					continue NEXT
				}
			}
		}
		begin++
	}
	sort.Slice(sameChunks, func(i, j int) bool { //sort by order
		return sameChunks[i].order < sameChunks[j].order
	})
	var o int
	//exclude sam chunks
	for _, chunk := range sameChunks { //collect diff orders
		for ; o < chunk.order; o++ {
			diffOrders = append(diffOrders, o)
		}
		o++
	}
	for ; o < len(sums); o++ {
		diffOrders = append(diffOrders, o)
	}
	return false, false, sameChunks, diffOrders
}

/* @note
 * 根据给定的diffOrders, 在文件中取出需要传输的数据块
 */
func (rf *RFile) GetDiffChunks(diffOrders []int) (blockLen int, totalSum []byte, diffChunks []RDiffChunk) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.blockLen == 0 {
		panic("invalid block length")
	}
	diffChunks = make([]RDiffChunk, len(diffOrders))
	for i, order := range diffOrders {
		diffChunks[i].Order = order
		d := cutByBlockLen(rf.data[order*rf.blockLen:], rf.blockLen)
		diffChunks[i].Data = make([]byte, len(d))
		copy(diffChunks[i].Data, d)
	}
	return rf.blockLen, rf.totalSum, diffChunks
}

/* @note
 * 根据已有的sameChunks和diffChunks，根据本地文件，拼出一个新文件
 * @return bool 表示是否替换成功，如果不成功则需要重新从头来过
 */
func (rf *RFile) AssembleByChunks(blockLen int, sameChunks []RSameChunk, diffChunks []RDiffChunk) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.blockLen == 0 || blockLen == 0 {
		panic("invalid block length")
	} else if rf.blockLen != blockLen {
		return false
	}
	sort.Slice(sameChunks, func(i, j int) bool { //sort by order
		return sameChunks[i].order < sameChunks[j].order
	})
	sort.Slice(diffChunks, func(i, j int) bool { //sort by order
		return diffChunks[i].Order < diffChunks[j].Order
	})
	sameChunkNum := len(sameChunks)
	diffChunkNum := len(diffChunks)
	totalChunkNum := sameChunkNum + diffChunkNum
	buf := bytes.NewBuffer(make([]byte, 0, totalChunkNum*blockLen))
	for cur, i, j := 0, 0, 0; cur < totalChunkNum; cur++ { //拼接
		for ; i < sameChunkNum && sameChunks[i].order < cur; i++ {
		}
		for ; j < diffChunkNum && diffChunks[j].Order < cur; j++ {
		}
		if cur == sameChunks[i].order {
			end := sameChunks[i].start + blockLen
			if end > len(rf.data) {
				end = len(rf.data)
			}
			buf.Write(cutByBlockLen(rf.data[sameChunks[i].start:], blockLen))
		} else if cur == diffChunks[j].Order {
			buf.Write(diffChunks[j].Data)
		} else {
			return false
		}
	}
	rf.data = buf.Bytes()
	rf.calBlockSum()
	return true
}
