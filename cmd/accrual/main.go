package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	log "github.com/sirupsen/logrus"
	"github.com/theplant/luhn"
	"golang.org/x/time/rate"

	"github.com/h1067675/gophermart/cmd/depository"
	"github.com/h1067675/gophermart/internal/configurer"
	"github.com/h1067675/gophermart/internal/logger"
)

// General structure
type Connect struct {
	Router     chi.Router
	Depository *depository.Storage
	Config     *configurer.Config
	Visitors   map[string]*rate.Limiter
	Mu         sync.Mutex
	Limit      int
	BanList    BanList
}

type BanList struct {
	Mu   sync.Mutex
	List map[string]time.Time
}

// Initialized general structure with a repositary and config
func InitializeRouter(dep *depository.Storage, conf *configurer.Config) *Connect {
	var c = Connect{
		Router:     chi.NewRouter(),
		Depository: dep,
		Config:     conf,
		Visitors:   make(map[string]*rate.Limiter),
		Limit:      100,
	}
	return &c
}

// Retrieve and return the rate limiter for the current visitor if it
// already exists. Otherwise create a new rate limiter and add it to
// the visitors map, using the IP address as the key.
func (c *Connect) getVisitor(ip string) *rate.Limiter {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	limiter, exists := c.Visitors[ip]
	if !exists {
		rt := rate.Every(time.Minute)
		limiter = rate.NewLimiter(rt, c.Limit)
		c.Visitors[ip] = limiter
	}

	return limiter
}

func (c *Connect) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			logger.Log.Debug()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		limiter := c.getVisitor(ip)

		if !limiter.Allow() {
			w.Header().Add("Retry-After", strconv.Itoa(int(limiter.Reserve().Delay()/time.Second)))
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(fmt.Sprintf("No more than %v requests per minute allowed", c.Limit)))
			return
		}

		next.ServeHTTP(w, r)
	})
}

// load user number order
func (c *Connect) AccrualHandler(response http.ResponseWriter, request *http.Request) {
	o := chi.URLParam(request, "order")
	if o == "" {
		logger.Log.Info("wrong order")
		http.Error(response, http.StatusText(http.StatusUnprocessableEntity), http.StatusUnprocessableEntity)
		return
	}

	order, err := strconv.Atoi(o)
	if err != nil {
		logger.Log.Info("wrong order")
		http.Error(response, http.StatusText(http.StatusUnprocessableEntity), http.StatusUnprocessableEntity)
		return
	}

	if !luhn.Valid(order) {
		logger.Log.Info("wrong order format (is not Luhn)")
		http.Error(response, http.StatusText(http.StatusUnprocessableEntity), http.StatusUnprocessableEntity)
		return
	}

	answer, err := c.Depository.OrderAccrual(order)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			logger.Log.Info("order not registred")
			response.WriteHeader(http.StatusNoContent)
			return
		}
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(answer)
	if err != nil {
		logger.Log.WithError(err).Error("error JSON marshal")
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Header().Add("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	logger.Log.Info("response write body:", string(body))
	response.Write(body)
}

// Routing http requests to edpoints
func (c *Connect) Route() chi.Router {
	// Use all middleware-functions
	c.Router.Use(logger.ResponseLogging)
	c.Router.Use(c.limit)

	// create routing
	c.Router.Route("/api/orders", func(r chi.Router) {
		r.Get("/{order}", c.AccrualHandler) // GET request for load user order to show list
	})
	logger.Log.Infof("Server is running %s", c.Config.GetRunAddress())
	return c.Router
}

func (c *Connect) StartServer() error {
	ctx := context.Background()
	defer ctx.Done()
	if err := http.ListenAndServe(c.Config.GetAccrualSystemAddress(), c.Route()); err != nil {
		logger.Log.WithError(err).Errorf("error starting the server with a network address %s", c.Config.GetAccrualSystemAddress())
		return err
	}
	return nil
}

func main() {
	logger.InitializeLogger(&log.JSONFormatter{}, log.InfoLevel, os.Stdout)
	var config configurer.Config
	conf := config.InitializeConfigurer("localhost:8080",
		"host=127.0.0.1 port=5432 dbname=postgres user=postgres password=12345678 connect_timeout=10 sslmode=prefer",
		"localhost:8090",
		false)
	var depositary = depository.InitializeStorager(conf)
	var connector = InitializeRouter(depositary, conf)
	connector.StartServer()

}
