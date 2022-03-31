package bytes

import (
	"fmt"
	//"github.com/dgraph-io/badger/v3"
	"bytes"
	"encoding/binary"
	"log"
	"math/bits"
	"testing"
)

/*
func Benchmark_Badger(b *testing.B) {

	opts := badger.DefaultOptions("C:/AllinFinal/Busquedas/db")
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	err = db.View(func(txn *badger.Txn) error {

		BadgerKey := []byte{1, 0, 0, 0, 1, 0}
		for i := 0; i < b.N; i++ {

			item, err := txn.Get(BadgerKey)
			if err == nil {
				val, err := item.ValueCopy(nil)
				if err == nil {
					m(val)
				}
				if err != nil {
					fmt.Println(err)
				}
			}
			if err != nil {
				fmt.Println(err)
			}

		}
		return nil

	})
	if err != nil {
		fmt.Println(err)
	}

}
func m(s []uint8) {

}
*/

func Benchmark_IntToBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IntToBytes(1)
	}
}
func Benchmark_IntToBytes2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encodeUint(1)
	}
}
func Benchmark_IntToBytes3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		a(1)
	}
}
func Benchmark_IntToBytes4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s(1)
	}
}

func IntToBytes(num int64) []byte {
	buff := new(bytes.Buffer)
	bigOrLittleEndian := binary.BigEndian
	err := binary.Write(buff, bigOrLittleEndian, num)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}
func encodeUint(x uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, x)
	return buf[bits.LeadingZeros64(x)>>3:]
}
func a(x uint32) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, x)
	return reverse(bs)
}

func reverse(numbers []uint8) []uint8 {
	for i := 0; i < len(numbers)/2; i++ {
		j := len(numbers) - i - 1
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
	return numbers
}
func s(l uint32) []byte {

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, l)
	if err != nil {
		fmt.Println(err)
	}
	return buf.Bytes()

}

//go test -benchmem -bench=.
