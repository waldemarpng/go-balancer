package balancer

import (
	"backend"
	"context"
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


type backendPool struct{
	backends []* backend.ServerNode
	current uint32
}

var (
	backendpool backendPool
	portsUsed []string
)

const (
	localhostUrl = "http://127.0.0.1"
	Retry int = 0
	Attempts int = 0
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
	for _, serverNode := range bp.backends{
		if serverNode.ProxyPort == port{
			serverNode.SetAlive(alive)
			break
		}
	}
}

func (bp *backendPool) getAliveCount() (count int){
	count = 0
	for _, serverNode := range bp.backends{
		if serverNode.IsAlive(){
			count +=1
		}
	}
	return
}

func initializeServerNode(port string, fromConfig bool){
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
	backendNode := &backend.ServerNode{
		ProxyPort: port,
		Alive: true,
		ReverseProxy: proxy,
	}
	if !fromConfig {
		/*
			Если передается значение из конфига, то мы не запускаем сервер в горутине
		*/
		backendServer := backendNode.CreateServer()
		go func() {
			backendServer.ListenAndServe()
		}()
	}
	backendpool.addContainer(backendNode)
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


func updatePorts(ports []string){
	mux := sync.RWMutex{}
	mux.RLock()
	portsUsed = ports
	mux.RUnlock()
}

func (bp *backendPool) initializePool(proxyPorts string){
	ports := strings.Split(proxyPorts, ";")
	if len(ports) == 0{
		log.Println("Don't find port from config parameters")
		return
	}
	for _, port := range ports{
		initializeServerNode(port, true)
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

func (bp *backendPool) addContainer(c *backend.ServerNode){
	bp.backends = append(bp.backends, c)
}


func (bp *backendPool) backendMapping()  *backend.ServerNode {
	next := bp.NextIndex()
	l := len(bp.backends) + next
	for i:=next; i < l; i++{
		idx := i % len(bp.backends)
		if bp.backends[idx].IsAlive(){
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
	urlServer, err := url.Parse(localhostUrl + backend.ProxyPort)
	if err != nil {
		log.Printf("Can't parse this url: %s", urlServer)
		return
	}
	log.Printf("Redirect to: %s", urlServer)
	if backend != nil{
		backend.ReverseProxy.ServeHTTP(w, r)
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
		backendUrl, err := url.Parse(localhostUrl + b.ProxyPort)
		if err != nil {
			log.Printf("Error when parsing url: %s%s", localhostUrl, b.ProxyPort)
			return
		}
		alive := isBackendAlive(backendUrl)
		b.SetAlive(alive)
		if !alive {
			status = "down"
			log.Printf("Backend down in: %s\n", time.Unix(b.DownUnixTime, 0))
		}
		log.Printf("%s [%s]\n", backendUrl, status)

	}
}

func healthCheck(){
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ticker.C:
			log.Println("Start heath check...")
			backendpool.healthCheck()
			aliveCount := backendpool.getAliveCount()
			if aliveCount < 2{
				port := generatePort()
				initializeServerNode(port, false)
			}
			backendpool.checkInactiveBackends()
			log.Println(portsUsed)

			log.Printf("Ready to accept connection: %d", aliveCount)
			log.Println("Stop health check...")
		}

	}
}


func (bp *backendPool) checkInactiveBackends(){
	for _, b := range bp.backends {
		nowUnixTime := time.Now().Unix()
		if nowUnixTime - b.DownUnixTime > 5 && !b.Alive{
			port := b.ProxyPort
			bp.deleteInactive(port)
			deletePort(port)
		}
	}
}


func updateBackends(backends []*backend.ServerNode){
	mux := sync.RWMutex{}
	mux.RLock()
	backendpool.backends = backends
	mux.RUnlock()

}

func deletePort(port string){
	portsSlice := getPorts()
	for idx, value := range portsSlice{
		if value == port{
			portsLen := len(portsSlice)
			portsSlice[idx] = portsSlice[portsLen - 1]
			portsSlice = portsSlice[:portsLen - 1]
			updatePorts(portsSlice)
			return
		}
	}
}

func (bp *backendPool) deleteInactive(port string){
	serverNodeSlice := bp.backends
	for idx, value := range serverNodeSlice{
		if value.ProxyPort == port{
			serverNodeLen := len(serverNodeSlice)
			serverNodeSlice[idx] = serverNodeSlice[serverNodeLen - 1]
			serverNodeSlice = serverNodeSlice[:serverNodeLen - 1]
			updateBackends(serverNodeSlice)
			return
		}
	}
}


func RunBalancer(){
	proxyPorts := ":8081"
	backendpool.initializePool(proxyPorts)
	go healthCheck()
	server := http.Server{
		Addr: ":3222",
		Handler: http.HandlerFunc(proxyToAlive),
	}
	server.ListenAndServe()
}