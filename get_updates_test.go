package bot

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"
)

type getUpdatesClientFunc func(*http.Request) (*http.Response, error)

func (f getUpdatesClientFunc) Do(req *http.Request) (*http.Response, error) {
	// drain the multipart body so the request pipe writer can finish
	_, _ = io.Copy(io.Discard, req.Body)
	_ = req.Body.Close()
	return f(req)
}

func getUpdatesJSONResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// One undecodable update must not poison the batch: the others are delivered,
// the offset moves past it and the failure is reported with the update id.
func Test_getUpdates_skipsUndecodableUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch := `{"ok":true,"result":[
		{"update_id":100,"message":{"message_id":1,"text":"first"}},
		{"update_id":101,"message":"not an object"},
		{"update_id":102,"message":{"message_id":2,"text":"third"}}
	]}`

	var calls int32
	var errs []string
	b := &Bot{
		token:         "XXX",
		updates:       make(chan *models.Update, 10),
		errorsHandler: func(handlerErr error) { errs = append(errs, handlerErr.Error()) },
		debugHandler:  func(string, ...any) {},
		client: getUpdatesClientFunc(func(*http.Request) (*http.Response, error) {
			if atomic.AddInt32(&calls, 1) > 1 {
				cancel()
				return nil, ctx.Err()
			}
			return getUpdatesJSONResponse(batch), nil
		}),
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	b.getUpdates(ctx, &wg)
	wg.Wait()

	if got := atomic.LoadInt64(&b.lastUpdateID); got != 102 {
		t.Fatalf("offset must move past the bad update, lastUpdateID=%d", got)
	}
	if len(b.updates) != 2 {
		t.Fatalf("expected 2 delivered updates, got %d", len(b.updates))
	}
	if first, third := <-b.updates, <-b.updates; first.ID != 100 || third.ID != 102 {
		t.Fatalf("expected updates 100 and 102, got %d and %d", first.ID, third.ID)
	}
	if len(errs) != 1 || !strings.Contains(errs[0], "101") || !strings.Contains(errs[0], "not an object") {
		t.Fatalf("undecodable update must be reported once with its id and raw payload, got %v", errs)
	}
}

// An update whose id cannot be read leaves the offset where it is, so the very same
// batch comes back on the next request. Without a backoff that is a tight loop.
func Test_getUpdates_backsOffWhenUpdateIDUnreadable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch := `{"ok":true,"result":[{"update_id":"abc","message":"x"}]}`

	var calls int
	var firstCallAt time.Time
	var gap time.Duration

	b := &Bot{
		token:         "XXX",
		updates:       make(chan *models.Update, 10),
		errorsHandler: func(error) {},
		debugHandler:  func(string, ...any) {},
		client: getUpdatesClientFunc(func(*http.Request) (*http.Response, error) {
			calls++
			switch calls {
			case 1:
				firstCallAt = time.Now()
			case 2:
				gap = time.Since(firstCallAt)
				cancel()
				return nil, ctx.Err()
			}
			return getUpdatesJSONResponse(batch), nil
		}),
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	b.getUpdates(ctx, &wg)
	wg.Wait()

	if gap < 100*time.Millisecond {
		t.Fatalf("expected a backoff before the next request, got %v", gap)
	}
}
