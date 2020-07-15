/**
 * @note
 * Adler32滚动校验
 *
 * @author	songtianming
 * @date 	2020-07-13
 */
package rsync

type RollSum struct {
	a, b     uint16
	xk       byte
	chunkLen int
}

func NewRollSum(chunkLen int) *RollSum {
	return &RollSum{
		a:        0,
		b:        0,
		chunkLen: chunkLen}
}

func (r *RollSum) Init(a uint16, b uint16, xk uint8) {
	r.a = a
	r.b = b
	r.xk = xk
}

func (r *RollSum) InitByBuf(buf []byte) {
	r.a, r.b = calWeakSum(buf)
	r.xk = buf[0]
	r.chunkLen = len(buf)
}

func (r *RollSum) Update(xk_1 byte, xl byte) {
	var a, b int

	a, b = int(r.a), int(r.b)
	a = a - int(r.xk) + int(xl)
	b = a + b - r.chunkLen*int(r.xk)
	r.xk = xk_1

	r.a = uint16(a)
	r.b = uint16(b)
}

func (r *RollSum) GetWeakSum() (uint16, uint16) {
	return r.a, r.b
}
