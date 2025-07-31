package main

import (
	"crypto/rand"
	"fmt"
	"log"
	mr "math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type params struct {
	id           string
	randomDelay  []time.Duration
	delays       []time.Duration
	codes        []int
	cutOffs      []int
	size         int
	bytesPerSec  *int
	isBinary     bool
	sessionCount int
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890 \n")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[mr.IntN(len(letterRunes))]
	}
	return string(b)
}

func (p *params) payload(size int) []byte {
	if p.isBinary {
		data := make([]byte, size)
		_, _ = rand.Read(data)
		return data
	} else {
		return []byte(randString(size))
	}
}

var sessions = make(map[string]int, 0)
var mx sync.Mutex

func (p *params) updateSession() {
	mx.Lock()
	p.sessionCount = sessions[p.id]
	sessions[p.id] = p.sessionCount + 1
	mx.Unlock()
}

func (p *params) delayDuration() time.Duration {
	if len(p.randomDelay) == 2 {
		return time.Duration(mr.Int64N(p.randomDelay[1].Milliseconds()-p.randomDelay[0].Milliseconds())+p.randomDelay[0].Milliseconds()) * time.Millisecond
	} else if len(p.delays) > 1 {
		idx := p.sessionCount
		if len(p.delays) <= idx {
			idx = len(p.delays) - 1
		}
		return p.delays[idx]
	} else if len(p.delays) == 1 {
		return p.delays[0]
	}
	return 0
}

func (p *params) delay() {
	dur := p.delayDuration()
	if dur.Nanoseconds() > 0 {
		log.Printf("delay: %s", dur)
		time.Sleep(dur)
	}
}

func (p *params) statusCode() int {
	if len(p.codes) > 0 {
		code := p.codes[len(p.codes)-1]
		if len(p.codes) > p.sessionCount {
			code = p.codes[p.sessionCount]
		}
		return code
	}
	return http.StatusOK
}

func (p *params) cutOff() int {
	if len(p.cutOffs) > 0 {
		cutOff := p.cutOffs[len(p.cutOffs)-1]
		if len(p.cutOffs) > p.sessionCount {
			cutOff = p.cutOffs[p.sessionCount]
		}
		return cutOff
	}
	return -1
}

func parseParams(q url.Values) (*params, error) {
	params := params{}
	{
		sd := q.Get("delay")
		if sd != "" {
			randDur := strings.Split(sd, "-")
			if len(randDur) == 2 {
				{
					delay, err := time.ParseDuration(randDur[0])
					if err != nil {
						return nil, fmt.Errorf("can't parse duration %q: %w", randDur[0], err)
					}
					params.randomDelay[0] = delay
				}
				{
					delay, err := time.ParseDuration(randDur[1])
					if err != nil {
						if err != nil {
							return nil, fmt.Errorf("can't parse duration %q: %w", randDur[1], err)
						}
					}
					params.randomDelay[1] = delay
				}
			} else {
				dur := strings.Split(sd, ",")
				for _, d := range dur {
					delay, err := time.ParseDuration(d)
					if err != nil {
						if err != nil {
							return nil, fmt.Errorf("can't parse duration %q: %w", d, err)
						}
					}
					params.delays = append(params.delays, delay)
				}
			}
		}
	}
	{
		ss := q.Get("size")
		if ss != "" {
			size, err := strconv.Atoi(ss)
			if err != nil {
				return nil, fmt.Errorf("can't parse size %q: %w", ss, err)
			}
			params.size = size
		} else {
			return nil, fmt.Errorf(`required "size" parameter is missing`)
		}
	}
	{
		sb := q.Get("bps")
		if sb != "" {
			bps, err := strconv.Atoi(sb)
			if err != nil {
				return nil, fmt.Errorf("can't parse bytes per second %q: %w", sb, err)
			}
			params.bytesPerSec = &bps
		}
	}
	{
		cutOffs := q.Get("cutOffs")
		for _, s := range strings.Split(cutOffs, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				cutOff, err := strconv.Atoi(s)
				if err != nil {
					return nil, fmt.Errorf("can't parse status codes %q: %w", cutOffs, err)
				}
				params.cutOffs = append(params.cutOffs, cutOff)
			}
		}
	}
	{
		codes := q.Get("codes")
		if codes != "" {
			for _, s := range strings.Split(codes, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					code, err := strconv.Atoi(s)
					if err != nil {
						return nil, fmt.Errorf("can't parse status codes %q: %w", codes, err)
					}
					params.codes = append(params.codes, code)
				}
			}
		}
	}
	params.isBinary = q.Has("bin")
	params.id = q.Get("id")
	return &params, nil
}

func handler() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "Streaming not supported by this connection", http.StatusInternalServerError)
				return
			}
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "Hijacking not supported by this connection", http.StatusInternalServerError)
				return
			}

			params, err := parseParams(r.URL.Query())
			if err != nil {
				http.Error(w, fmt.Sprintf("Error parsing query string: %s", err.Error()), http.StatusBadRequest)
				return
			}

			params.updateSession()
			params.delay()

			code := params.statusCode()
			if code/100 != 2 {
				http.Error(w, http.StatusText(code), code)
				return
			}
			w.WriteHeader(code)

			if params.isBinary {
				w.Header().Add("Content-Type", "application/octet-stream")
			} else {
				w.Header().Add("Content-Type", "text/plain; charset=us-ascii")
			}
			w.Header().Add("Content-Length", strconv.Itoa(params.size))

			toSend := params.size
			chunkSize := params.size
			if params.bytesPerSec != nil {
				chunkSize = *params.bytesPerSec
			}
			data := params.payload(chunkSize)
			cutOff := params.cutOff()
			for toSend > 0 {
				size := len(data)
				if toSend < size {
					size = toSend
				}
				if cutOff >= 0 {
					if cutOff < size {
						size = cutOff
					}
					cutOff -= size
				}
				_, _ = w.Write(data[:size])
				toSend -= size
				if toSend > 0 {
					flusher.Flush()
					if cutOff == 0 {
						conn, _, _ := hijacker.Hijack()
						_ = conn.Close()
						break
					}
					time.Sleep(time.Second)
				}
			}
		},
	)
}

func main() {
	mux := http.NewServeMux()
	mux.Handle("/", handler())

	port := 7778
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}

	log.Printf("Address: http://127.0.0.1:%d\n", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
	if err != nil {
		log.Fatalln(err)
	}

}
