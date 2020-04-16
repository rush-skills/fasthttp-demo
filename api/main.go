package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
)

var (
	addr     = flag.String("addr", ":8080", "TCP address to listen to")
	compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
	b bytes.Buffer
	//f *os.File
	//bw *bufio.Writer
)

type Writer int
func (*Writer) Write(p []byte) (n int, err error) {
	fmt.Println(len(p))
	fmt.Println(p)
	return len(p), nil
}

func main() {
	flag.Parse()

	h := requestHandler
	if *compress {
		h = fasthttp.CompressHandler(h)
	}

	//go bufferHandler()
	//
	//w := new(Writer)
	//bw := bufio.NewWriterSize(w, 100)

	f, err := os.Create("log.txt")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

//func bufferHandler() {
//	for {
//		if bw.Size() > 100 {
//			err := bw.Flush()
//			if err != nil {
//				panic(err)
//			}
//		}
//	}
//}

func requestHandler(ctx *fasthttp.RequestCtx) {
	body := ctx.PostBody()
	//fmt.Println( "Your json is: " + string(body))
	clientId := fastjson.GetInt(body, "client_id")

	timestamp := fastjson.GetString(body, "timestamp")
	t, err := time.Parse("2006/01/02 15:04:05.000", timestamp)
	if err != nil {
		fmt.Println(err)
	}
	date := t.Format("2006-01-02")
	//fmt.Println("clientId:", clientId)
	//fmt.Println("YYYY-MM-DD:", date)

	filePath := "/chat/" + date + "/content_logs_" + date + "_" + strconv.Itoa(clientId)
	//fmt.Println(filePath)

	fmt.Fprintf(ctx, "OK")
	fmt.Fprintf(ctx, filePath)
	//bw.Write(body)
	//bw.Write([]byte("\n"))
}