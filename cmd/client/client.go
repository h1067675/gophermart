package client

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	mu              sync.Mutex
	TooManyRequests map[string]TooManyRequests
}
type TooManyRequests struct {
	ChDone    chan struct{}
	retryTime time.Time
}

type HTTPClient interface {
	Client
	GET(server string, endpoint string, order int) (body []byte, status int, timeout int, err error)
	GETtest(server string, endpoint string) (body []byte, status int, err error)
	POSTtest(server string, endpoint string, requestBody string, contentType string, cookies *[]*http.Cookie) (body []byte, status int, err error)
}

var ErrTooManyRequests = errors.New("too many requests to server")

func (c *Client) init() {
	c.TooManyRequests = make(map[string]TooManyRequests)
}
func (c *Client) checkBan(s string) bool {
	_, ok := c.TooManyRequests[s]
	return ok
}

func (c *Client) createBan(s string, ra time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.checkBan(s) {
		return
	}
	c.TooManyRequests[s] = TooManyRequests{ChDone: make(chan struct{}), retryTime: time.Now().Add(ra)}
	go func() {
		timer := time.NewTimer(time.Until(c.TooManyRequests[s].retryTime))
		<-timer.C
		c.mu.Lock()
		defer c.mu.Unlock()
		close(c.TooManyRequests[s].ChDone)
		delete(c.TooManyRequests, s)
	}()
}

func (c *Client) GET(server string, endpoint string, order int) (body []byte, status int, timeout int, err error) {
	client := &http.Client{}
	request, err := http.NewRequest(http.MethodGet, "http://"+server+endpoint+strconv.Itoa(order), nil)
	if err != nil {
		return
	}
	if c.checkBan(server) {
		err = ErrTooManyRequests
		return
	}
	response, err := client.Do(request)
	if err != nil {
		return
	}
	retry := response.Header.Get("Retry-After")
	if retry != "" {
		timeout, err = strconv.Atoi(retry)
		if err != nil {
			return
		}
		c.createBan(server, time.Second*time.Duration(timeout))
		err = ErrTooManyRequests
		return
	}
	defer response.Body.Close()
	body, err = io.ReadAll(response.Body)
	if err != nil {
		return
	}
	return body, response.StatusCode, timeout, nil
}

func (c *Client) GETtest(server string, endpoint string) (body []byte, status int, err error) {
	client := &http.Client{}
	request, err := http.NewRequest(http.MethodGet, "http://"+server+endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, 0, err
	}
	return body, response.StatusCode, nil
}

func (c *Client) POSTtest(server string, endpoint string, requestBody string, contentType string, cookies *[]*http.Cookie) (body []byte, status int, err error) {
	client := &http.Client{}
	request, err := http.NewRequest(http.MethodPost, "http://"+server+endpoint, strings.NewReader(contentType))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Add("Content-Type", contentType)
	for _, e := range *cookies {
		request.AddCookie(e)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, 0, err
	}

	*cookies = append(*cookies, response.Cookies()...)
	return body, response.StatusCode, nil
}
