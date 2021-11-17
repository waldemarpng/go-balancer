package balancer

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Controller struct{
	URL *url.URL
	Alive bool
	mux sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

type controllerPool struct {
	controllers []*Controller
	current uint64
}

func (s *controllerPool) NextIndex() int{
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.controllers)))
}

// GetNextPeer Returns next active controller to take a connection
func (c *controllerPool) GetNextPeer() *Controller {
	next := c.NextIndex()
	l := len(c.controllers) + next
	for i := next; i < l; i++{
		idx := i % len(c.controllers)

		if c.controllers[idx].IsAlive(){
			if i != next{
				atomic.StoreUint64(&c.current, uint64(idx))
			}
			return c.controllers[idx]
		}
	}
	return nil
}


func (c *Controller) SetAlive(alive bool){
	c.mux.Lock()
	c.Alive = alive
	c.mux.Unlock()
}

func (c *Controller) IsAlive() (alive bool){
	c.mux.RLock()
	alive = c.Alive
	c.mux.RUnlock()
	return
}

func GetRetryFromContext(r *http.Request) int{
	if retry, ok := r.Context().Value(Retry).(int); ok{
		return retry
	}
	return 0
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok{
		return attempts
	}
	return 1
}

func (c *controllerPool) MarkControllerStatus(controllerUrl *url.URL, alive bool){
	for _, controller := range c.controllers{
		if controller.URL.String() == controllerUrl.String(){
			controller.SetAlive(alive)
			break
		}
	}
}

// checks whether a controller is Alive by establishing a TCP connection
func isControllerAlive(u *url.URL) bool{
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Println("Site unreachable, error: ", err)
		return false
	}
	_ = conn.Close()
	return true
}

func (c *controllerPool) AddBackend(controller *Controller){
	c.controllers = append(c.controllers, controller)
}

// HealthCheck pings the controllers and update the status
func (s *controllerPool) HealthCheck(){
	for _, c := range s.controllers {
		status := "up"
		alive := isControllerAlive(c.URL)
		c.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", c.URL, status)
	}
}

func healthCheck() {
	t := time.NewTicker(time.Second * 20)
	for {
		select {
		case <-t.C:
		log.Println("Starting  health check...")
		ControlPOOL.HealthCheck()
		log.Println("Health check completed")
		}
	}
}

func controllerBalancer(w http.ResponseWriter, r *http.Request){
	attempts := GetAttemptsFromContext(r)
	if attempts > 3{
		log.Printf("%s(%s) Max attempts reached terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}
	peer := ControlPOOL.GetNextPeer()
	if peer != nil{
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

var ControlPOOL controllerPool

func startControllerBalancer(){
	controllerList := "http://127.0.0.1:8080,http://127.0.0.1:8081,"

	tokens := strings.Split(controllerList, ",")
	for _, tok := range tokens {
		controllerUrl, err := url.Parse(tok)
		if err != nil{
			log.Fatal(err)
		}
		proxy := httputil.NewSingleHostReverseProxy(controllerUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", controllerUrl.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}
			ControlPOOL.MarkControllerStatus(controllerUrl, false)


			attempts := GetAttemptsFromContext(request)
			log.Printf("%s (%s) Attemping retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), attempts, attempts+1)
			controllerBalancer(writer, request.WithContext(ctx))
		}
		ControlPOOL.AddBackend(&Controller{
			URL: controllerUrl,
			Alive: true,
			ReverseProxy: proxy,
		})
	}
	server := http.Server{
		Addr: ":3222",
		Handler: http.HandlerFunc(controllerBalancer),
	}
	go healthCheck()
	if err:= server.ListenAndServe(); err != nil{
		log.Fatal(err)
	}
}

