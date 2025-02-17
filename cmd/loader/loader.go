package loader

import (
	"container/heap"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/h1067675/gophermart/cmd/client"
	"github.com/h1067675/gophermart/cmd/depository"
	"github.com/h1067675/gophermart/internal/logger"
)

type Loader struct {
	Config     Config
	QueryLimit QueryLimit
	HTTPClient *client.Client
	Depository *depository.Storage
	Mutex      Mutex
	Quere      Quere
}

type Config struct {
	Server      string
	Periodicity time.Duration
	Workers     int
}

type QueryLimit struct {
	Mu          sync.Mutex
	QueryLimit  int
	QueryTicker int
}

type Mutex struct {
	Pause     bool
	ChPause   chan struct{}
	StartTime time.Time
	sync.Mutex
}

type Quere struct {
	PriorityQueue PriorityQueue
	Buffer        Buffer
	SecondBuffer  Buffer
	OldBuffer     Buffer
	NewOrders     chan Order
	Processing    chan Order
	Result        chan Order
}

type Buffer struct {
	Orders []Order
	Mu     sync.Mutex
}
type Order struct {
	Order   int
	Status  string
	Times   int
	Accrual float64
}

func (l *Loader) wait(s string) {
	logger.Log.Info("mutex: check pause")
	if t, ok := l.HTTPClient.TooManyRequests[s]; ok {
		logger.Log.Info("mutex: pause is true, wait context")
		<-t.ChDone
		logger.Log.Info("mutex: context getting, return")
		return
	}
	logger.Log.Info("mutex: pause is false")

}

func InitializeLoader(depository *depository.Storage, server string, periodicity time.Duration, workers int) *Loader {
	var c client.Client
	var loader = Loader{
		QueryLimit: QueryLimit{
			QueryLimit: 1000,
		},
		Config: Config{
			Server:      server,
			Periodicity: periodicity,
			Workers:     workers,
		},
		Depository: depository,
		Quere: Quere{
			NewOrders:  make(chan Order, 100),
			Processing: make(chan Order, 100),
			Result:     make(chan Order, 100),
		},
		Mutex: Mutex{
			ChPause: make(chan struct{}),
		},
		HTTPClient: &c,
	}
	return &loader
}

func (o *Order) getPriority() int {
	switch {
	case o.Times <= 3:
		return 1
	case 3 < o.Times && o.Times <= 6:
		return 2
	case 6 < o.Times:
		return 3
	}
	return 1
}

func (l *Loader) uploader(worker int, jobs <-chan Order, results chan<- Order) {
	logger.Log.Infof("loader: start worker %v", worker)
	for j := range jobs {
		start := time.Now()
		logger.Log.Infof("loader: order received %v to worker %v", j.Order, worker)
		res, err := l.NgetOrderStatusFromServerAPI(j)
		if err != nil || res.Status != depository.OrderProcessed {
			heap.Push(&l.Quere.PriorityQueue, &Item{value: j, priority: j.getPriority(), index: len(l.Quere.PriorityQueue)})
		} else {
			results <- res
			logger.Log.Infof("loader: responce received %v", res)
		}
		logger.Log.Infof("loader: time work %v, worker %v", time.Since(start), worker)
	}
}

func (l *Loader) StartLoaderWorkers() {
	for w := 1; w <= l.Config.Workers; w++ {
		go l.uploader(w, l.Quere.Processing, l.Quere.Result)
	}

	for j := range l.Quere.Result {
		go l.NupdateOrder(j)
	}
}

type responseCalculator struct {
	Order   string  `json:"order"`
	Status  string  `json:"status"`
	Accrual float64 `json:"accrual,omitempty"`
}

func (q *QueryLimit) addQuery() {
	logger.Log.Infof("loader: try to get 1 query from %v, busy %v", q.QueryLimit, q.QueryTicker)
	if q.QueryTicker == q.QueryLimit {
		logger.Log.Infof("loader: query limit %v per seccond is full, ", q.QueryLimit)
		q.addQuery()
	}
	q.add()
}
func (q *QueryLimit) setNewQueryLimit(i int) {
	q.Mu.Lock()
	defer q.Mu.Unlock()
	q.QueryLimit = i
}

func (q *QueryLimit) queryTickerTimer() {
	time.Sleep(time.Second * 60)
	q.remove()
}

func (q *QueryLimit) add() {
	logger.Log.Infof("loader: allowed 1 query from %v, busy %v", q.QueryLimit, q.QueryTicker)
	q.Mu.Lock()
	logger.Log.Infof("loader: QueryLimit: mutex.Lock()")
	defer func() {
		q.Mu.Unlock()
		logger.Log.Infof("loader: QueryLimit: mutex.Unlock()")
	}()
	q.QueryTicker++
}

func (q *QueryLimit) remove() {
	logger.Log.Infof("loader: free 1 query from %v, busy %v", q.QueryLimit, q.QueryTicker)
	q.Mu.Lock()
	logger.Log.Infof("loader: QueryLimit: mutex.Lock()")
	defer func() {
		q.Mu.Unlock()
		logger.Log.Infof("loader: QueryLimit: mutex.Unlock()")
	}()
	q.QueryTicker--
}
func getCountAllowedRequestsPerMinuteFromResponse(body string) (int, error) {
	s := strings.Replace(string(body), "No more than ", "", -1)
	s = strings.Replace(s, " requests per minute allowed", "", -1)
	lim, err := strconv.Atoi(s)
	if err != nil {
		logger.Log.Errorf("number parse error %s", err)
		return -1, err
	}
	return lim, nil
}

func (l *Loader) checkResponseStatus(status int, body []byte, timeout int) *responseCalculator {
	switch status {
	case http.StatusTooManyRequests:
		logger.Log.Infof("loader: answer - get response with status Too Many Requests and timeout %v", timeout)
		lim, err := getCountAllowedRequestsPerMinuteFromResponse(string(body))
		if err == nil {
			logger.Log.Infof("loader:set new query limit is %v times in second", lim)
			l.QueryLimit.setNewQueryLimit(lim)
		}
		return nil
	case http.StatusNoContent:

	case http.StatusOK:
		var js responseCalculator
		err := json.Unmarshal(body, &js)
		if err != nil {
			logger.Log.WithError(err).Error("loader: json parsing error")
			return nil
		}
		_, err = strconv.Atoi(js.Order)
		if err != nil {
			logger.Log.WithError(err).Error("loader: order is not number")
			return nil
		}
		return &js
	}
	return nil
}
func (l *Loader) NgetOrderStatusFromServerAPI(order Order) (result Order, err error) {
	result = order
	logger.Log.Infof("loader: query - GET %s/api/orders/%v", l.Config.Server, order)
	l.wait(l.Config.Server)
	l.QueryLimit.addQuery()
	go l.QueryLimit.queryTickerTimer()
	body, HTTPStatus, timeout, err := l.HTTPClient.GET(l.Config.Server, "/api/orders/", order.Order)
	if err != nil {
		logger.Log.WithError(err).Error("error getting status from outer sistem")
		return
	}
	result.Times++
	if js := l.checkResponseStatus(HTTPStatus, body, timeout); js != nil {
		if js.Status == depository.OrderProcessed {
			result.Status = depository.OrderProcessed
			if js.Accrual > 0 {
				result.Accrual = js.Accrual
			}
			return
		}
	}
	return
}

func (l *Loader) NupdateOrder(ch Order) {
	logger.Log.Infof("loader: data is received from the channel")
	tx, err := l.Depository.DB.Begin()
	if err != nil {
		return
	}
	logger.Log.Infof("loader: DB transaction begin")
	if ch.Status != "" {
		err = l.Depository.OrderStatusUpdate(ch.Order, ch.Status, ch.Accrual, tx)
		if err != nil {
			tx.Rollback()
		}
		var userID int
		userID, err = l.Depository.OrderUserCheck(ch.Order)
		if err != nil {
			tx.Rollback()
		}
		if ch.Accrual > 0 {
			err = l.Depository.UserBalanceUpdate(userID, ch.Accrual, 0, tx)
			if err != nil {
				tx.Rollback()
			}
		}
	}
	tx.Commit()
	logger.Log.Infof("loader: DB transaction commit")
}

// func (b *Buffer) read() (res Order, r bool) {
// 	if len(b.Orders) > 0 {
// 		b.Mu.Lock()
// 		defer b.Mu.Unlock()
// 		res = b.Orders[0]
// 		if len(b.Orders) > 1 {
// 			b.Orders = b.Orders[1 : len(b.Orders)-1]
// 		} else {
// 			b.Orders = b.Orders[:0]
// 		}
// 		r = true
// 	}
// 	return
// }

// func (b *Buffer) write(i Order) {
// 	b.Mu.Lock()
// 	defer b.Mu.Unlock()
// 	b.Orders = append(b.Orders, i)
// }

func (q *Quere) sendOrderToProcessing(o Order) bool {
	if len(q.Processing) < 100 {
		logger.Log.Infof("loader: order %v from buffer send to processing channel", o.Order)
		q.Processing <- o
		return true
	}
	return false
}
func (l *Loader) sendNewOrdersToQuere() {
	for o := range l.Quere.NewOrders {
		if !l.Quere.sendOrderToProcessing(o) {
			logger.Log.Infof("loader: order %v from neworders can't send to processing channel and write to heap", o.Order)
			heap.Push(&l.Quere.PriorityQueue, &Item{value: o, priority: o.getPriority(), index: len(l.Quere.PriorityQueue)})
		}
	}
}

func (l *Loader) sendOrdersFromBufferToProcessing() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		var max int
		if cap(l.Quere.Processing)-10 > len(l.Quere.PriorityQueue) {
			max = len(l.Quere.PriorityQueue)
		} else {
			max = cap(l.Quere.Processing) - 10
		}
		for i := len(l.Quere.Processing); i < max; i++ {
			e := heap.Pop(&l.Quere.PriorityQueue).(*Item)
			if !l.Quere.sendOrderToProcessing(e.value) {
				logger.Log.Infof("loader: order %v from heap can't send to processing channel and write to heap", e)
				heap.Push(&l.Quere.PriorityQueue, &e)
				l.Quere.PriorityQueue.update(e, e.value, e.value.getPriority())
			}
			if len(l.Quere.Processing) > i {
				i = len(l.Quere.Processing)
			}
		}
	}
}

func (l *Loader) QuereManager() {
	go l.sendNewOrdersToQuere()
	go l.sendOrdersFromBufferToProcessing()
}
