/**
 * @note
 * rsync_test
 *
 * @author	songtianming
 * @date 	2020-07-13
 */
package rsync

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func generateRandomBuf(l int) []byte {
	buf := make([]byte, l)
	for i := range buf {
		buf[i] = uint8(rand.Intn(math.MaxUint8))
	}
	return buf
}

func Test_CalCheckSumByChunkLen(t *testing.T) {
	buf := generateRandomBuf(1<<16 + 64)
	checkSum := calCheckSumByBlockLen(buf, 1<<8)
	for _, c := range checkSum {
		fmt.Println(c.Order, c.A, c.B, string(c.SS))
	}
}

func Test_Rsync(t *testing.T) {
	//生成初始数据
	dataSize := 1<<20 + 64  //原始数据大小 1M
	blockSize := 1 << 9     //用于计算校验值的数据块大小 512B
	fragmentSize := 1 << 10 //随机插入的数据块大小 1K
	fragmentNum := 32       //随机插入的数据块数量 32个

	buf := generateRandomBuf(dataSize)
	newBuf := buf
	//生成新数据
	var actualNewDataSize int
	for i := 0; i < fragmentNum; i++ {
		fragment := generateRandomBuf(fragmentSize)
		actualNewDataSize += len(fragment)
		j := rand.Intn(dataSize)
		newBuf = append(append(newBuf[:j], fragment...), newBuf[j:]...)
	}
	//新数据为server
	server := NewRFile(blockSize) //512B block
	server.SetData(newBuf, true)
	//老数据为client
	client := NewRFile(blockSize) //512B block
	client.SetData(buf, false)
BEGIN:
	//从server处获得文件校验信息
	firstServerBlockLen, firstTotalSum, serverBlockSum := server.GetProperties()
	//client比较文件校验信息
	same, reset, sameChunks, diffOrders := client.CheckByCheckSum(firstServerBlockLen, firstTotalSum, serverBlockSum)
	if same { //如果完全一样，则直接返回
		return
	} else if reset { //两边的blockLen发生不一致，需要重新算
		client.SetBlockLen(firstServerBlockLen)
		goto BEGIN
	}
	//再次从server处，通过diffOrders获得对应数据块
	secondServerBlockLen, secondTotalSum, diffChunks := server.GetDiffChunks(diffOrders)
	//注意第二次要先对比从server处两次获得信息的totalSum是否一致,不一致要从头来
	if !CompareBytes(firstTotalSum, secondTotalSum) {
		goto BEGIN
	}
	//client对获得的文件进行拼接，如果出现问题则要重来
	success := client.AssembleByChunks(secondServerBlockLen, sameChunks, diffChunks)
	if !success {
		goto BEGIN
	}
	var transferDataSize int //计算一下总传输的数据量
	for _, diffChunk := range diffChunks {
		transferDataSize += len(diffChunk.Data)
	}
	fmt.Println("total data size:", len(newBuf))               //总共的数据量
	fmt.Println("actual new data size:", actualNewDataSize)    //插入的新数据量
	fmt.Println("checksum data size:", 30*len(serverBlockSum)) //校验值的总大小
	fmt.Println("transfer data size:", transferDataSize)       //传输的数据大小
	if CompareBytes(client.GetData(), server.GetData()) {      //比较一下是否真的同步成功
		fmt.Println("rsync success")
	}
	return
}
