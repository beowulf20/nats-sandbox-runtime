package app

import (
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/micro"
)

func logMicroRequest(endpoint string, handler micro.HandlerFunc) micro.HandlerFunc {
	return func(req micro.Request) {
		start := time.Now()
		loggedReq := &microRequestLogger{Request: req}
		handler(loggedReq)
		elapsed := time.Since(start)

		if loggedReq.err != nil {
			log.Printf("request endpoint=%s subject=%s result=error error=%q duration=%s", endpoint, req.Subject(), loggedReq.err.Error(), elapsed)
			return
		}
		if loggedReq.errorCode != "" {
			log.Printf("request endpoint=%s subject=%s result=error code=%s duration=%s", endpoint, req.Subject(), loggedReq.errorCode, elapsed)
			return
		}
		if !loggedReq.completed {
			log.Printf("request endpoint=%s subject=%s result=error error=%q duration=%s", endpoint, req.Subject(), "no response sent", elapsed)
			return
		}
		log.Printf("request endpoint=%s subject=%s result=success duration=%s", endpoint, req.Subject(), elapsed)
	}
}

type microRequestLogger struct {
	micro.Request

	completed bool
	errorCode string
	err       error
}

func (r *microRequestLogger) Respond(data []byte, opts ...micro.RespondOpt) error {
	err := r.Request.Respond(data, opts...)
	r.completed = true
	r.err = err
	return err
}

func (r *microRequestLogger) RespondJSON(value any, opts ...micro.RespondOpt) error {
	err := r.Request.RespondJSON(value, opts...)
	r.completed = true
	r.err = err
	return err
}

func (r *microRequestLogger) Error(code, description string, data []byte, opts ...micro.RespondOpt) error {
	err := r.Request.Error(code, description, data, opts...)
	r.completed = true
	r.errorCode = code
	r.err = err
	return err
}

func logHTTPRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		loggedWriter := &httpResponseLogger{ResponseWriter: w}
		handler.ServeHTTP(loggedWriter, r)

		status := loggedWriter.status
		if status == 0 {
			status = http.StatusOK
		}
		result := "success"
		if status >= http.StatusBadRequest {
			result = "error"
		}
		log.Printf("http_request method=%s path=%s result=%s status=%d duration=%s", r.Method, r.URL.Path, result, status, time.Since(start))
	})
}

type httpResponseLogger struct {
	http.ResponseWriter

	status int
}

func (w *httpResponseLogger) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *httpResponseLogger) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *httpResponseLogger) Flush() {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}
