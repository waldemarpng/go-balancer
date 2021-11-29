package backend

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"
)

type ServerNode struct{
	ProxyPort string
	Alive bool
	ReverseProxy *httputil.ReverseProxy
	mux sync.RWMutex
	DownUnixTime int64
}

func (sn *ServerNode) SetAlive(alive bool){
	sn.mux.Lock()
	sn.Alive = alive
	if !alive && sn.DownUnixTime == 0{
		sn.DownUnixTime = time.Now().Unix()
	}
	sn.mux.Unlock()
}

func (sn *ServerNode) IsAlive() bool{
	sn.mux.RLock()
	defer sn.mux.RUnlock()
	return sn.Alive
}

func (sn *ServerNode) CreateServer() *http.Server{
	/*
		Создаем мультиплексор
		При вызове ListenAndServe используется стандартный, поэтому нам нужно его переопределить,
		чтобы при создании горутины они не обращались к одному
	*/
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprintf(writer, sn.ProxyPort)
	})
	return &http.Server{
		Addr: sn.ProxyPort,
		Handler: mux,
	}
}

