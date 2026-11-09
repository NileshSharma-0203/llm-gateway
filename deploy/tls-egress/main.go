// tls-egress accepts plaintext TCP on local ports and forwards each connection
// over TLS to a fixed upstream. The gateway has no TLS library, so this
// sidecar handles certificate verification for it. Bind it to loopback or a
// private network only: traffic between the gateway and the sidecar is plaintext.
//
// Usage: tls-egress 127.0.0.1:9001=api.openai.com:443 127.0.0.1:9002=api.anthropic.com:443
package main

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const dialTimeout = 5 * time.Second

func forward(ctx context.Context, client net.Conn, target, serverName string) {
	defer client.Close()
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second},
		Config:    &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
	}
	upstream, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		log.Printf("dial %s: %v", target, err)
		return
	}
	defer upstream.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(upstream, client)
		upstream.(*tls.Conn).CloseWrite()
	}()
	go func() {
		defer wg.Done()
		io.Copy(client, upstream)
		client.(*net.TCPConn).CloseWrite()
	}()
	wg.Wait()
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: tls-egress <listen>=<host:port> ...")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	for _, spec := range os.Args[1:] {
		listen, target, ok := strings.Cut(spec, "=")
		if !ok {
			log.Fatalf("invalid mapping %q", spec)
		}
		serverName, _, err := net.SplitHostPort(target)
		if err != nil {
			log.Fatalf("invalid target %q: %v", target, err)
		}
		listener, err := net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("listen %s: %v", listen, err)
		}
		log.Printf("forwarding %s to %s", listen, target)
		go func() {
			<-ctx.Done()
			listener.Close()
		}()
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("accept: %v", err)
					time.Sleep(50 * time.Millisecond)
					continue
				}
				go forward(ctx, conn, target, serverName)
			}
		}()
	}
	<-ctx.Done()
}
