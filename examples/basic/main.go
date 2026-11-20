// Command basic connects two peers on localhost and echoes a message over a stream.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	kolotransport "github.com/ciranda-dev/kolo-transport"
)

const label = "kolo-example/session/v0"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l, err := kolotransport.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	serverKey := make(chan [32]byte, 1)
	go echo(ctx, l, serverKey)

	c, err := kolotransport.Dial(ctx, l.Addr().String(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	s, err := c.OpenStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := s.Write([]byte("hello kolo")); err != nil {
		log.Fatal(err)
	}
	s.Close()
	reply, err := io.ReadAll(s)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("echo: %s\n", reply)

	key, err := c.ExportKey(label)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("both ends agree on the session key:", key == <-serverKey)
}

func echo(ctx context.Context, l *kolotransport.Listener, keys chan<- [32]byte) {
	c, err := l.Accept(ctx)
	if err != nil {
		return
	}
	key, err := c.ExportKey(label)
	if err != nil {
		return
	}
	keys <- key
	s, err := c.AcceptStream(ctx)
	if err != nil {
		return
	}
	io.Copy(s, s)
	s.Close()
}
