package balancer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)


type backend struct{
	proxyPort string
	alive bool
	reverseProxy *httputil.ReverseProxy
	mux sync.RWMutex
	downUnixTime int64
}

type backendPool struct{
	backends []*backend
	current uint32
}

var (
	backendpool backendPool
	portsUsed []string
)

const (
	dockerFilePath = "../Docker/dockerfile"
	localhostUrl = "http://127.0.0.1"
	Retry int = 0
	Attempts int = 0
	minBackends int = 3
)

func contains(s[] string, str string) bool {
	for _, v := range s{
		if v == str{
			return true
		}
	}
	return false
}

func (bp *backendPool) NextIndex() int{
	return int(atomic.AddUint32(&bp.current, uint32(1)) % uint32(len(bp.backends)))
}

func (c *backend) setAlive(alive bool){
	c.mux.Lock()
	c.alive = alive
	if !alive && c.downUnixTime == 0{
		c.downUnixTime = time.Now().Unix()
	}
	c.mux.Unlock()
}

func (c*backend) isAlive() bool{
	c.mux.RLock()
	defer c.mux.RUnlock()
	return c.alive
}

func GetAttemptsFromContext(r *http.Request)int{
	if attempts, ok := r.Context().Value(Attempts).(int); ok{
		return attempts
	}
	return 0
}

func GetRetryFromContext(r *http.Request)int{
	if retry, ok := r.Context().Value(Retry).(int); ok{
		return retry
	}
	return 0
}

func (bp *backendPool) markBackendStatus(port string, alive bool){
	for _, backend := range bp.backends{
		if backend.proxyPort == port{
			backend.setAlive(alive)
			break
		}
	}
}

func (bp *backendPool) getAliveCount() (count int){
	count = 0
	for _, backend := range bp.backends{
		if backend.isAlive(){
			count +=1
		}
	}
	return
}

func initializeBackend(port string){
	log.Printf("backend with port[%s] was added", port)
	urlServer, err := url.Parse(localhostUrl + port)
	if err != nil {
		log.Printf("Can't parse this url: %s", urlServer)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(urlServer)
	proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, err error) {
		log.Printf("[%s] %s\n", urlServer, err.Error())
		retries := GetRetryFromContext(request)
		if retries > 3 {
			select {
			case <-time.After(10 * time.Millisecond):
				ctx := context.WithValue(request.Context(), Retry, retries+1)
				proxy.ServeHTTP(writer, request.WithContext(ctx))
			}
			return
		}
		backendpool.markBackendStatus(port, false)

		attempts := GetAttemptsFromContext(request)
		log.Printf("%s (%s) Attemping retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
		ctx := context.WithValue(request.Context(), attempts, attempts+1)
		proxyToAlive(writer, request.WithContext(ctx))
	}
	backendpool.addContainer(&backend{
		proxyPort: port,
		alive: true,
		reverseProxy: proxy,
	})
	appendPort(port)
}

func appendPort(port string){
	mux := sync.RWMutex{}
	mux.RLock()
	portsUsed = append(portsUsed, port)
	mux.RUnlock()
}

func getPorts() []string{
	mux := sync.RWMutex{}
	mux.Lock()
	ports :=portsUsed
	mux.Unlock()
	return ports
}

// TODO: Поменять проверку удаления
func updateBackends(backends []*backend){
	mux := sync.RWMutex{}
	mux.RLock()
	backendpool.backends = backends
	mux.RUnlock()

}

// TODO: Поменять проверку удаления
func updatePorts(ports []string){
	mux := sync.RWMutex{}
	mux.RLock()
	portsUsed = ports
	mux.RUnlock()

}

// TODO: Поменять проверку удаления
func deletePort(port string){
	ports:=getPorts()
	for idx, value := range ports{
		if value == port{
			lenPorts := len(ports)
			ports[idx] = ports[lenPorts - 1]
			ports[lenPorts-1] = " "
			ports = ports[:lenPorts -1 ]
			updatePorts(ports)
			return
		}
	}
}

// TODO: Поменять проверку удаления
func deleteBackend(port string){
	backends := backendpool.backends
	for idx, value := range backends{
		if value.proxyPort == port{
			lenBackends := len(backends)
			backends[idx] = backends[lenBackends - 1]
			backends[lenBackends-1] = nil
			backends = backends[:lenBackends -1 ]
			updateBackends(backends)
			return
		}
	}
}

func (bp *backendPool) initializePool(proxyPorts string){
	ports := strings.Split(proxyPorts, ";")
	if len(ports) == 0{
		log.Println("Don't find port from config parameters")
		return
	}
	for _, port := range ports{
		initializeBackend(port)
	}
	return
}

func generatePort() string{
	rand.Seed(time.Now().UnixNano())
	min:=9000
	max:=1000
	port := ":"+strconv.Itoa(rand.Intn(max)+min)
	log.Printf("Random port [%s]", port)
	found := contains(getPorts(), port)
	if !found{
		return port
	}
	return ""
}

func (bp *backendPool) addContainer(c *backend){
	bp.backends = append(bp.backends, c)
}


func (bp *backendPool) backendMapping()  *backend {
	next := bp.NextIndex()
	l := len(bp.backends) + next
	for i:=next; i < l; i++{
		idx := i % len(bp.backends)
		if bp.backends[idx].isAlive(){
			if i != next{
				atomic.StoreUint32(&bp.current, uint32(idx))
			}
			return bp.backends[idx]
		}
	}
	return nil
}


func proxyToAlive(w http.ResponseWriter, r *http.Request){
	backend := backendpool.backendMapping()
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}
	urlServer, err := url.Parse(localhostUrl + backend.proxyPort)
	if err != nil {
		log.Printf("Can't parse this url: %s", urlServer)
		return
	}
	log.Printf("Redirect to: %s", urlServer)
	if backend != nil{
		backend.reverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

func isBackendAlive(u *url.URL) bool{
	timeout := 4 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)

	if err!=nil{
		log.Printf("Service unreachable url: %s with err: %s", u.Host, err)
		return false
	}
	err = conn.Close()
	if err!=nil{
		log.Printf("Can't close connection")
	}
	return true
}

func (bp *backendPool) healthCheck(){
	for _, b := range bp.backends{
		status := "ready"
		backendUrl, err := url.Parse(localhostUrl + b.proxyPort)
		if err != nil {
			log.Printf("Error when parsing url: %s%s", localhostUrl, b.proxyPort)
			return
		}
		alive := isBackendAlive(backendUrl)
		b.setAlive(alive)
		if !alive {
			status = "down"
			log.Printf("Backend down in: %s\n", time.Unix(b.downUnixTime, 0))
		}
		log.Printf("%s [%s]\n", backendUrl, status)

	}
}

func healthCheck(portChannel chan string){
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ticker.C:
			log.Println("Start heath check...")
			backendpool.healthCheck()
			aliveCount := backendpool.getAliveCount()
			if aliveCount < 2{
				port := generatePort()
				go initializeBackend(port)
				portChannel <-port
			}
			backendpool.checkInactiveBackends()
			log.Println(portsUsed)

			log.Printf("Ready to accept connection: %d", aliveCount)
			log.Println("Stop health check...")
		}

	}
}


func runBackend(backendPorts chan string){
	for{
		select {
		case port := <-backendPorts:
			log.Printf("Try to init backend with port: %s", port)
			http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
					fmt.Fprintf(writer, port)
			})
			log.Fatal(http.ListenAndServe("localhost"+port, nil))
		}
	}
}

func (bp *backendPool) checkInactiveBackends(){
	for _, b := range bp.backends {
		nowUnixTime := time.Now().Unix()
		if nowUnixTime - b.downUnixTime > 5 && !b.alive{
			// TODO: Поменять проверку удаления
			port := b.proxyPort
			deleteBackend(port)
			deletePort(port)
		}
	}
}


func RunBalancer(){
	proxyPorts := ":8081"
	portChannel := make(chan string, 2)
	backendpool.initializePool(proxyPorts)
	go healthCheck(portChannel)
	go runBackend(portChannel)
	server := http.Server{
		Addr: ":3222",
		Handler: http.HandlerFunc(proxyToAlive),
	}
	server.ListenAndServe()

}