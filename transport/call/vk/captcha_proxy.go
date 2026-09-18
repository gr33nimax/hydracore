package vk

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// A page that cannot be fetched inside these bounds is reported as a failure rather than waited
// on: the core gives the whole question 120 seconds, and a WebView with nothing to show is not a
// state a person can act on.
const (
	captchaDialTimeout     = 45 * time.Second
	captchaUpstreamTimeout = 60 * time.Second
	// The window a person has to answer, and what one request is allowed to cost: the first bounds
	// the wait, the second keeps an upstream page from becoming a memory ceiling nobody set.
	captchaWindow            = 120 * time.Second
	captchaMaxBodyBytes      = 8 << 20
	captchaServerReadTimeout = 60 * time.Second
)

// CaptchaOutcome says why a wait for the answer ended.
//
// An empty token alone cannot: a question whose window ran out, a question the person closed and a
// proxy that died underneath them mean different things to the caller — one ends the attempt, the
// others may be asked again.
type CaptchaOutcome int

const (
	CaptchaSolved CaptchaOutcome = iota
	// CaptchaCancelled: the proxy was stopped while the question was open.
	CaptchaCancelled
	// CaptchaTimedOut: the window ran out with nobody answering.
	CaptchaTimedOut
	// CaptchaContextCancelled: the wait was cancelled — a closed question or a torn-down attempt.
	CaptchaContextCancelled
	// CaptchaProxyFailed: the local server stopped serving, so the question cannot be answered.
	CaptchaProxyFailed
)

// CaptchaResult is the answer to one wait: the token, if it was solved, and why the wait ended.
type CaptchaResult struct {
	Token   string
	Outcome CaptchaOutcome
}

var activeCaptchaProxy struct {
	sync.Mutex
	listener net.Listener
	server   *http.Server
	port     int
	keyCh    chan string
	doneCh   chan struct{}
	stopOnce *sync.Once
	failed   bool
}

func StartCaptchaProxy(redirectURI string, dialer N.Dialer, logger logger.ContextLogger) int {
	StopCaptchaProxy()
	targetURL, err := url.Parse(redirectURI)
	if err != nil {
		return 0
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	port := listener.Addr().(*net.TCPAddr).Port
	localOrigin := fmt.Sprintf("http://127.0.0.1:%d", port)
	upstreamOrigin := targetURL.Scheme + "://" + targetURL.Host
	keyCh := make(chan string, 1)
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: captchaUpstreamTimeout,
	}
	if dialer != nil {
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialCtx, cancel := context.WithTimeout(ctx, captchaDialTimeout)
			defer cancel()
			return dialer.DialContext(dialCtx, network, M.ParseSocksaddr(addr))
		}
	} else {
		transport.DialContext = (&net.Dialer{Timeout: captchaDialTimeout}).DialContext
	}
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// A white page with no explanation is the one outcome a person cannot act on: say what
			// failed, so the overlay can show it and the journal keeps it.
			if logger != nil {
				logger.Warn("vk-auth: captcha page fetch failed: ", err)
			}
			http.Error(w, "captcha page unavailable: "+err.Error(), http.StatusBadGateway)
		},
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL.Scheme = targetURL.Scheme
			req.Out.URL.Host = targetURL.Host
			if req.Out.URL.Path == "" {
				req.Out.URL.Path = targetURL.Path
			}
			req.Out.Host = targetURL.Host
			req.Out.Header.Del("Accept-Encoding")
			req.Out.Header.Del("TE")
			for _, headerName := range []string{"Origin", "Referer"} {
				val := req.Out.Header.Get(headerName)
				if val != "" {
					req.Out.Header.Set(headerName, strings.ReplaceAll(val, localOrigin, upstreamOrigin))
				}
			}
		},
		ModifyResponse: func(res *http.Response) error {
			rewriteProxyCookies(res)
			if res.StatusCode >= 300 && res.StatusCode < 400 {
				if loc := res.Header.Get("Location"); loc != "" {
					res.Header.Set("Location", strings.ReplaceAll(loc, upstreamOrigin, localOrigin))
				}
			}
			contentType := res.Header.Get("Content-Type")
			shouldInspect := isHTMLLike(contentType) || strings.Contains(res.Request.URL.Path, "captchaNotRobot.check")
			if !shouldInspect {
				return nil
			}
			reader := res.Body
			decompressed := false
			if res.Header.Get("Content-Encoding") == "gzip" {
				gzReader, err := gzip.NewReader(res.Body)
				if err == nil {
					reader = gzReader
					decompressed = true
					defer gzReader.Close()
				}
			}
			// Bounded on purpose: the page is rewritten in memory, so an unbounded read is a ceiling
			// nobody chose.
			bodyBytes, err := io.ReadAll(io.LimitReader(reader, captchaMaxBodyBytes))
			if err != nil {
				return err
			}
			if len(bodyBytes) >= captchaMaxBodyBytes {
				return fmt.Errorf("captcha page is larger than %d bytes", captchaMaxBodyBytes)
			}
			res.Body.Close()
			if strings.Contains(res.Request.URL.Path, "captchaNotRobot.check") {
				token := extractSuccessToken(bodyBytes)
				if token != "" {
					select {
					case keyCh <- token:
					default:
					}
				}
			}
			if isHTMLLike(contentType) {
				for _, h := range []string{
					"Content-Security-Policy", "Content-Security-Policy-Report-Only",
					"X-Content-Security-Policy", "X-WebKit-CSP",
					"Cross-Origin-Opener-Policy", "Cross-Origin-Embedder-Policy",
					"Cross-Origin-Resource-Policy", "X-Frame-Options",
					"Strict-Transport-Security", "Alt-Svc",
				} {
					res.Header.Del(h)
				}
				bodyBytes = []byte(rewriteCaptchaHTML(string(bodyBytes), localOrigin, upstreamOrigin))
			}
			if decompressed {
				res.Header.Del("Content-Encoding")
			}
			res.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			res.ContentLength = int64(len(bodyBytes))
			res.Header.Set("Content-Length", fmt.Sprint(len(bodyBytes)))
			return nil
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/local-captcha-result", func(w http.ResponseWriter, r *http.Request) {
		token := r.FormValue("token")
		if token != "" {
			select {
			case keyCh <- token:
			default:
			}
		}
		// Only the page this proxy serves posts a token here: it is same-origin, so the header
		// names that origin instead of leaving every site on the device free to call it.
		w.Header().Set("Access-Control-Allow-Origin", localOrigin)
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/generic_proxy", func(w http.ResponseWriter, r *http.Request) {
		proxyURL := r.URL.Query().Get("proxy_url")
		parsed, err := url.Parse(proxyURL)
		if err != nil || parsed.Host == "" {
			http.Error(w, "Bad URL", http.StatusBadRequest)
			return
		}
		genericProxy := &httputil.ReverseProxy{
			Transport:    transport,
			ErrorHandler: proxy.ErrorHandler,
			Rewrite: func(req *httputil.ProxyRequest) {
				req.Out.URL.Scheme = parsed.Scheme
				req.Out.URL.Host = parsed.Host
				req.Out.URL.Path = parsed.Path
				req.Out.URL.RawQuery = parsed.RawQuery
				req.Out.Host = parsed.Host
				req.Out.Header.Del("Accept-Encoding")
			},
		}
		genericProxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && targetURL.Path != "" && targetURL.Path != "/" && r.URL.RawQuery == "" {
			localPath := targetURL.Path
			if targetURL.RawQuery != "" {
				localPath += "?" + targetURL.RawQuery
			}
			http.Redirect(w, r, localPath, http.StatusTemporaryRedirect)
			return
		}
		proxy.ServeHTTP(w, r)
	})
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       captchaServerReadTimeout,
		WriteTimeout:      captchaUpstreamTimeout + 30*time.Second,
		IdleTimeout:       60 * time.Second,
	}
	activeCaptchaProxy.Lock()
	activeCaptchaProxy.listener = listener
	activeCaptchaProxy.server = server
	activeCaptchaProxy.port = port
	activeCaptchaProxy.keyCh = keyCh
	activeCaptchaProxy.doneCh = make(chan struct{})
	activeCaptchaProxy.stopOnce = &sync.Once{}
	activeCaptchaProxy.failed = false
	activeCaptchaProxy.Unlock()
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			if logger != nil {
				logger.Warn("vk-auth: captcha proxy stopped serving: ", err)
			}
			// Nobody can answer through a server that is gone: end the wait instead of letting the
			// core sit out its window with no way to say why.
			captchaProxyFailed()
		}
	}()
	return port
}

func GetCaptchaResultContext(ctx context.Context, timeout time.Duration) CaptchaResult {
	activeCaptchaProxy.Lock()
	ch := activeCaptchaProxy.keyCh
	done := activeCaptchaProxy.doneCh
	activeCaptchaProxy.Unlock()
	if ch == nil || done == nil {
		return CaptchaResult{Outcome: CaptchaCancelled}
	}
	select {
	case token := <-ch:
		return CaptchaResult{Token: token, Outcome: CaptchaSolved}
	case <-done:
		activeCaptchaProxy.Lock()
		failed := activeCaptchaProxy.failed
		activeCaptchaProxy.Unlock()
		if failed {
			return CaptchaResult{Outcome: CaptchaProxyFailed}
		}
		return CaptchaResult{Outcome: CaptchaCancelled}
	case <-ctx.Done():
		return CaptchaResult{Outcome: CaptchaContextCancelled}
	case <-time.After(timeout):
		return CaptchaResult{Outcome: CaptchaTimedOut}
	}
}

// captchaProxyFailed ends the wait when the proxy itself is gone.
//
// Closing the channel twice would panic, so the stop path and this one share one `Once`.
func captchaProxyFailed() {
	activeCaptchaProxy.Lock()
	if activeCaptchaProxy.doneCh == nil {
		activeCaptchaProxy.Unlock()
		return
	}
	activeCaptchaProxy.failed = true
	once := activeCaptchaProxy.stopOnce
	done := activeCaptchaProxy.doneCh
	activeCaptchaProxy.Unlock()
	if once != nil {
		once.Do(func() { close(done) })
	}
}

func StopCaptchaProxy() {
	activeCaptchaProxy.Lock()
	ln := activeCaptchaProxy.listener
	server := activeCaptchaProxy.server
	done := activeCaptchaProxy.doneCh
	once := activeCaptchaProxy.stopOnce
	activeCaptchaProxy.listener = nil
	activeCaptchaProxy.server = nil
	activeCaptchaProxy.port = 0
	activeCaptchaProxy.keyCh = nil
	activeCaptchaProxy.doneCh = nil
	activeCaptchaProxy.stopOnce = nil
	activeCaptchaProxy.failed = false
	activeCaptchaProxy.Unlock()
	if done != nil && once != nil {
		once.Do(func() { close(done) })
	}
	if server != nil {
		server.Close()
	}
	if ln != nil {
		ln.Close()
	}
}

func rewriteProxyCookies(res *http.Response) {
	cookies := res.Cookies()
	if len(cookies) == 0 {
		return
	}
	res.Header.Del("Set-Cookie")
	for _, cookie := range cookies {
		cookie.Domain = ""
		cookie.Secure = false
		cookie.Partitioned = false
		if cookie.SameSite == http.SameSiteNoneMode || cookie.SameSite == http.SameSiteStrictMode {
			cookie.SameSite = http.SameSiteLaxMode
		}
		res.Header.Add("Set-Cookie", cookie.String())
	}
}

func isHTMLLike(contentType string) bool {
	return strings.Contains(contentType, "text/html") ||
		strings.Contains(contentType, "application/xhtml+xml")
}

func extractSuccessToken(body []byte) string {
	var payload struct {
		Response struct {
			SuccessToken string `json:"success_token"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return payload.Response.SuccessToken
}

func rewriteCaptchaHTML(html, localOrigin, upstreamOrigin string) string {
	html = strings.ReplaceAll(html, upstreamOrigin, localOrigin)
	script := fmt.Sprintf(`
<script>
(function() {
    var localOrigin = %q;
    var upstreamOrigin = %q;

    function rewriteUrl(urlStr) {
        if (!urlStr || typeof urlStr !== 'string') return urlStr;
        if (urlStr.indexOf(localOrigin) === 0) return urlStr;
        if (urlStr.indexOf(upstreamOrigin) === 0) return localOrigin + urlStr.slice(upstreamOrigin.length);
        if (urlStr.indexOf('//') === 0) {
            return '/generic_proxy?proxy_url=' + encodeURIComponent(window.location.protocol + urlStr);
        }
        if (urlStr.indexOf('http://') === 0 || urlStr.indexOf('https://') === 0) {
            return '/generic_proxy?proxy_url=' + encodeURIComponent(urlStr);
        }
        return urlStr;
    }

    function rewriteElementAttr(el, attr) {
        if (!el || !el.getAttribute) return;
        var value = el.getAttribute(attr);
        if (!value) return;
        var rewritten = rewriteUrl(value);
        if (rewritten !== value) el.setAttribute(attr, rewritten);
    }

    function rewriteDocument(root) {
        if (!root || !root.querySelectorAll) return;
        root.querySelectorAll('[href]').forEach(function(el) { rewriteElementAttr(el, 'href'); });
        root.querySelectorAll('[src]').forEach(function(el) { rewriteElementAttr(el, 'src'); });
        root.querySelectorAll('form[action]').forEach(function(el) { rewriteElementAttr(el, 'action'); });
    }

    function handleSuccessToken(token) {
        if (!token) return;
        fetch('/local-captcha-result', {
            method: 'POST',
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            body: 'token=' + encodeURIComponent(token)
        }).catch(function() {});
    }

    var origOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function() {
        if (arguments[1] && typeof arguments[1] === 'string') {
            this._origUrl = arguments[1];
            arguments[1] = rewriteUrl(arguments[1]);
        }
        return origOpen.apply(this, arguments);
    };
    var origSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function() {
        var xhr = this;
        if (this._origUrl && this._origUrl.indexOf('captchaNotRobot.check') !== -1) {
            xhr.addEventListener('load', function() {
                try {
                    var data = JSON.parse(xhr.responseText);
                    if (data.response && data.response.success_token) handleSuccessToken(data.response.success_token);
                } catch (e) {}
            });
        }
        return origSend.apply(this, arguments);
    };

    var origFetch = window.fetch;
    if (origFetch) {
        window.fetch = function() {
            var url = arguments[0];
            var urlStr = (typeof url === 'object' && url && url.url) ? url.url : url;
            var origUrlStr = urlStr;
            if (typeof urlStr === 'string') {
                urlStr = rewriteUrl(urlStr);
                arguments[0] = urlStr;
            }
            var p = origFetch.apply(this, arguments);
            if (typeof origUrlStr === 'string' && origUrlStr.indexOf('captchaNotRobot.check') !== -1) {
                p.then(function(r) { return r.clone().json(); }).then(function(data) {
                    if (data.response && data.response.success_token) handleSuccessToken(data.response.success_token);
                }).catch(function() {});
            }
            return p;
        };
    }

    var origWindowOpen = window.open;
    if (origWindowOpen) {
        window.open = function(url) {
            if (typeof url === 'string') arguments[0] = rewriteUrl(url);
            return origWindowOpen.apply(this, arguments);
        };
    }

    rewriteDocument(document);
    if (document.documentElement && window.MutationObserver) {
        new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.type === 'attributes' && mutation.target) {
                    rewriteElementAttr(mutation.target, mutation.attributeName);
                    return;
                }
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === 1) rewriteDocument(node);
                });
            });
        }).observe(document.documentElement, {
            subtree: true, childList: true, attributes: true,
            attributeFilter: ['href', 'src', 'action']
        });
    }
})();
</script>
`, localOrigin, upstreamOrigin)
	if idx := strings.Index(html, "</head>"); idx >= 0 {
		return html[:idx] + script + html[idx:]
	}
	if idx := strings.Index(html, "</body>"); idx >= 0 {
		return html[:idx] + script + html[idx:]
	}
	return html + script
}
