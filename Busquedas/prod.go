package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/valyala/fasthttp"
)

type Result struct {
}
type Config struct {
	Tiempo time.Duration `json:"Tiempo"`
}
type Metric struct {
	Count     uint32    `json:"Count"`
	LastCount uint32    `json:"LastCount"`
	Fecha     time.Time `json:"Fecha"`
	Lista     []uint8   `json:"Lista"`
}
type MyHandler struct {
	Conf    Config             `json:"Conf"`
	Count   []uint8            `json:"Count"`
	Prods   map[uint32][]uint8 `json:"Prods"`
	Empresa map[uint32][]uint8 `json:"Empresa"`
	Minimo  uint32             `json:"Minimo"`
	Metrics Metric             `json:"Metric"`
	Db      *badger.DB         `json:"Db"`
}

func main() {

	db := GetDb()

	pass := &MyHandler{
		Conf:    Config{},
		Count:   make([]byte, 100000),
		Prods:   make(map[uint32][]uint8, 0),
		Empresa: make(map[uint32][]uint8, 0),
		Minimo:  1,
		Db:      db,
		Metrics: Metric{Count: 0, LastCount: 0, Lista: make([]uint8, 100), Fecha: time.Now()},
	}

	pass.Empresa[1] = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	pass.Prods[1] = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	/*
		for i := uint32(1); i <= 1000; i++ {
			for j := uint16(1); j <= 1000; j++ {
				key := append(int32tobytes(i), int16tobytes(j)...)
				pass.SaveDb(key, []byte{0, 1, 0, 1, 1, 3, 0, 1, 2, 2, 0, 1, 2, 2, 0, 1, 2, 2})
			}
		}
	*/

	fmt.Println("GUARDADO")

	con := context.Background()
	con, cancel := context.WithCancel(con)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGHUP)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()
	go func() {
		for {
			select {
			case s := <-signalChan:
				switch s {
				case syscall.SIGHUP:
					pass.Conf.init()
				case os.Interrupt:
					cancel()
					os.Exit(1)
				}
			case <-con.Done():
				log.Printf("Done.")
				os.Exit(1)
			}
		}
	}()
	go func() {
		fasthttp.ListenAndServe(":81", pass.HandleFastHTTP2)
	}()
	if err := run(con, pass, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

}

func (h *MyHandler) SaveDb(key []uint8, value []uint8) bool {

	txn := h.Db.NewTransaction(true) // Read-write txn
	err := txn.SetEntry(badger.NewEntry(key, value))
	if err != nil {
		panic(err)
	}
	err = txn.Commit()
	if err != nil {
		panic(err)
	}
	return true

}

type Filtros struct {
	N uint16   `json:"N"`
	V []uint16 `json:"V"`
}
type Evals struct {
	N uint16 `json:"N"`
	V uint16 `json:"V"`
}

func GetCuad(param []uint8) ([]uint16, error) {
	var arrcuad []uint16
	if err := json.Unmarshal(param, &arrcuad); err == nil {
		return arrcuad, nil
	} else {
		return nil, err
	}
}

func GetDb() *badger.DB {

	var opts badger.Options

	if runtime.GOOS == "windows" {
		opts = badger.DefaultOptions("C:/AllinFinal/BadgerDb")
	} else {
		opts = badger.DefaultOptions("/var/db")
	}

	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()
	return db

}

func (h *MyHandler) HandleFastHTTP2(ctx *fasthttp.RequestCtx) {

	if string(ctx.Method()) == "GET" {
		switch string(ctx.Path()) {
		case "/":

			cat := ctx.QueryArgs().Peek("cat")
			h.Metric(GetParamUint32(cat))

			cuads, err := GetCuad(ctx.QueryArgs().Peek("cuads"))
			if err == nil {

				err = h.Db.View(func(txn *badger.Txn) error {

					for _, element := range cuads {

						item, err := txn.Get(append(Read_uint32bytes(cat), int16tobytes(element)...))
						if err == nil {
							val, err := item.ValueCopy(nil)
							if err == nil {
								fmt.Println(val)
							}
						}

					}
					return nil
				})

			}

		default:
			ctx.Error("Not Found", fasthttp.StatusNotFound)
		}
	}

}

func (h *MyHandler) HandleFastHTTP(ctx *fasthttp.RequestCtx) {

	var arrcuad []uint16
	var result Result
	var filtros []Filtros
	var evals [][]uint16

	if string(ctx.Method()) == "GET" {
		switch string(ctx.Path()) {
		case "/":

			now := time.Now()

			cat := ctx.QueryArgs().Peek("cat")
			h.Metric(GetParamUint32(cat))
			var id_prod uint32 = 0
			var precio uint32 = 0

			if err := json.Unmarshal(ctx.QueryArgs().Peek("filtros"), &filtros); err == nil {
				fmt.Println("filtros")
				fmt.Println(filtros)
			} else {
				fmt.Println(err)
			}
			if err := json.Unmarshal(ctx.QueryArgs().Peek("evals"), &evals); err == nil {
				fmt.Println("evals")
				fmt.Println(evals)
			} else {
				fmt.Println(err)
			}

			if err := json.Unmarshal(ctx.QueryArgs().Peek("cuads"), &arrcuad); err == nil {

				fmt.Println("cuads")
				fmt.Println(arrcuad)

				for _, element := range arrcuad {
					BadgerKey := append(Read_uint32bytes(cat), int16tobytes(element)...)
					err = h.Db.View(func(txn *badger.Txn) error {
						item, err := txn.Get(BadgerKey)
						if err == nil {
							val, err := item.ValueCopy(nil)
							if err == nil {

								length := len(val)
								j, e := 0, 0

								for {

									if length <= j {
										break
									}

									id_emp, c := GetSize2(val[j : j+3])
									id_loc, d := GetSize1(val[j+c : j+c+2])
									j = j + c + d

									if res, found := h.Empresa[uint32(id_emp)]; found {

										loc_pos := id_loc * 8
										lat := Float32frombytes(res[loc_pos : loc_pos+4])
										lng := Float32frombytes(res[loc_pos+4 : loc_pos+8])

										if distance := Distance(-33.44546, 70.44546, lat, lng); distance > 0 {

											cantarr := val[j : j+1][0]
											j++

											for i := uint8(0); i < cantarr; i++ {

												_, size_id, size_precio, tipoid := DecTipo(val[j : j+1][0])
												cantprod, w := GetSize1(val[j+1 : j+3])
												j = j + w + 1

												for s := uint64(0); s < cantprod; s++ {

													id_prod = GetProd(val[j:j+4], size_id)
													j = j + int(size_id) + 2

													precio = GetPrecio(val[j:j+4], size_precio)
													j = j + int(size_precio) + 2

													if tipoid == 0 {

													}
													if tipoid == 1 {
														if prod, found := h.Prods[id_prod]; found {
															pj := 0
															cantfiltros, v := GetSize1(prod[pj : pj+2])
															pj = pj + v
															for y := uint64(0); y < cantfiltros; y++ {
																// ACA QUEDAMOS
															}

														} else {
															fmt.Println("PRODUCTO1 NOT FOUND", id_prod)
														}
													}

												}

											}

										}

									} else {
										fmt.Println("EMPRESA NOT FOUND", id_emp)
									}
									e++

								}

							}
						}
						return nil
					})
					check(err)
				}
				fmt.Fprintf(ctx, result.Print())
			} else {
				fmt.Println(err)
				fmt.Fprintf(ctx, "Error Unmarshal")
			}

			elapsed := time.Since(now)
			fmt.Printf("ELAPSED %v\n", elapsed.Nanoseconds())

		default:
			ctx.Error("Not Found", fasthttp.StatusNotFound)
		}
	}

}
func GetProd(val []uint8, size_id uint8) uint32 {

	switch size_id {
	case 0:
		return Bytes2toInt32(val[0:2])
	case 1:
		return Bytes3toInt32(val[0:3])
	case 2:
		return Bytes4toInt32(val[0:4])
	default:
		return 0
	}

}
func GetPrecio(val []uint8, size_precio uint8) uint32 {
	switch size_precio {
	case 0:
		return Bytes2toInt32(val[0:2])
	case 1:
		return Bytes3toInt32(val[0:3])
	case 2:
		return Bytes4toInt32(val[0:4])
	default:
		return 0
	}
}

/*
func (r *Result) Add(bytes []byte, distancia uint32) {

	var j int = 0
	cantarr := bytes[j : j+1][0]
	idemp, c := GetSize2(bytes[j+1 : j+5])
	j = 1 + c

	for i := uint8(0); i < cantarr; i++ {

	}

	size_id, size_precio, tipo := Getsizes(int(size))

	switch tipo {
	case 1:
		//tipo 1

		for m := 0; m < 256; m++ {

			fmt.Printf("SID: %v - SPR %v - TIPO %v - SIZE %v\n", size_id, size_precio, tipo, size)
		}

		for i := uint8(0); i < cantarr; i++ {
		}

	case 2:
		//tipo 2

	default:

	}
}
*/
func (r *Result) Print() string {
	return ""
}
func Getsizes(size int) (a int, b int, c int) {

	c = size / 12
	d := size % 12

	a = d % 3
	b = d / 3

	return
}

// DAEMON //
func (h *MyHandler) StartDaemon() {

	h.Conf.Tiempo = 1 * time.Second
}
func (c *Config) init() {
	var tick = flag.Duration("tick", 1*time.Second, "Ticking interval")
	c.Tiempo = *tick
}
func run(con context.Context, c *MyHandler, stdout io.Writer) error {
	c.Conf.init()
	log.SetOutput(os.Stdout)
	for {
		select {
		case <-con.Done():
			return nil
		case <-time.Tick(c.Conf.Tiempo):
			c.StartDaemon()
		}
	}
}

// DAEMON //
func GetParamUint32(data []byte) uint32 {
	var x uint32
	for _, c := range data {
		x = x*10 + uint32(c-'0')
	}
	return x
}
func Read_uint32bytes(data []byte) []byte {
	var x uint32
	for _, c := range data {
		x = x*10 + uint32(c-'0')
	}
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, x)
	return b
}
func int16tobytes(i uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, i)
	return b
}
func int32tobytes(i uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, i)
	return b
}
func GetSize1(buf []byte) (size uint64, count int) {
	size = uint64(buf[0:1][0])
	if size == 255 {
		size = Bytes2toInt64(buf[0:2])
		if size == 65535 {
			size = Bytes3toInt64(buf[0:3])
			return size - 2, 3
		}
		return size - 1, 2
	}
	return size, 1
}
func GetSize2(buf []byte) (size uint64, count int) {
	size = Bytes2toInt64(buf[0:2])
	if size == 65535 {
		size = Bytes3toInt64(buf[0:3])
		return size - 2, 3
	}
	return size, 2
}
func Bytes3toInt64(b []uint8) uint64 {
	bytes := make([]byte, 5, 8)
	bytes = append(bytes, b...)
	return binary.BigEndian.Uint64(bytes)
}
func Bytes3toInt32(b []uint8) uint32 {
	bytes := make([]byte, 1, 4)
	bytes = append(bytes, b...)
	return binary.BigEndian.Uint32(bytes)
}
func Bytes2toInt64(b []uint8) uint64 {
	bytes := make([]byte, 6, 8)
	bytes = append(bytes, b...)
	return binary.BigEndian.Uint64(bytes)
}
func Float32frombytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}
func Distance(lat1, lng1, lat2, lng2 float32) uint32 {
	first := math.Pow(float64(lat2-lat1), 2)
	second := math.Pow(float64(lng2-lng1), 2)
	return uint32(math.Sqrt(first + second))
}
func check(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
func GuardarVisitas(cat uint32) {
	// GUARDAR INFORMACION - VERIFICAR SI DEBE SER CACHE
}
func (h *MyHandler) Metric(cat uint32) {

	h.Metrics.Count++
	p := (cat - h.Minimo) * 2

	if h.Count[p+1 : p+2][0] < 255 {
		h.Count[p+1 : p+2][0]++
	} else {
		if h.Count[p : p+1][0] < 255 {
			h.Count[p : p+1][0]++
			h.Count[p+1 : p+2][0] = 0
		} else {
			GuardarVisitas(cat)
			h.Count[p+0 : p+1][0] = 0
			h.Count[p+1 : p+2][0] = 0
		}
	}
}
func Bytes2toInt32(b []uint8) uint32 {
	bytes := make([]byte, 2, 4)
	bytes = append(bytes, b...)
	return binary.BigEndian.Uint32(bytes)
}
func Bytes1toInt32(b []uint8) uint32 {
	bytes := make([]byte, 3, 4)
	bytes = append(bytes, b...)
	return binary.BigEndian.Uint32(bytes)
}
func Bytes4toInt32(b []uint8) uint32 {
	return binary.BigEndian.Uint32(b)
}
func DecTipo(b uint8) (tipo uint8, sid uint8, sprecio uint8, tipoid uint8) {

	tipo = b / 24
	aux1 := b % 24
	sid = aux1 / 8
	aux2 := aux1 % 8
	sprecio = aux2 / 2
	tipoid = aux2 % 2
	return
}
func DecTipo2(b uint8) (tipo uint8, sid uint8, sprecio uint8, tipoid uint8) {

	tipo = b / 24
	aux1 := b % 24
	sid = aux1 / 8
	aux2 := aux1 % 8
	sprecio = aux2 / 2
	tipoid = aux2 % 2
	return
}
func DecTipo3(b uint8) (Tipo uint8, CantPrecio uint8, CantId uint8, Eval uint8, Filtro uint8, TipoId uint8) {

	Tipo = b / 96
	aux1 := b % 96
	CantPrecio = aux1 / 24
	aux2 := aux1 % 24
	CantId = aux2 / 8
	aux3 := aux2 % 8
	Eval = aux3 / 4
	aux4 := aux3 % 4
	Filtro = aux4 / 2
	aux5 := aux4 % 2
	TipoId = aux5 % 2
	return

}
