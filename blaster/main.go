package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
)

func main() {
	strPost := []byte("POST")
	strRequestURI := []byte("http://localhost:8080")

	//billion := 1000000000
	for i := 0; i <= 1000000; i++ {
		req := fasthttp.AcquireRequest()
		clientId := rand.Intn(10) + 1
		now := time.Now()
		json := "{\"text\": \"hello world\", \"content_id\": " +
			strconv.Itoa(i) +
			", \"client_id\": " + strconv.Itoa(clientId) +
			", \"timestamp\": \"" + now.Format("2006/01/02 15:04:05.000") + "\"}"
		//fmt.Println(json)
		req.SetBody([]byte(json))
		req.Header.SetMethodBytes(strPost)
		req.SetRequestURIBytes(strRequestURI)
		res := fasthttp.AcquireResponse()
		if err := fasthttp.Do(req, res); err != nil {
			panic("error in sending request: " + err.Error())
		}
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(res)
	}
}
