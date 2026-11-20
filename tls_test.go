package kolotransport

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"testing"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

func TestTLSConfigs(t *testing.T) {
	cert, err := newCertificate()
	if err != nil {
		t.Fatal(err)
	}
	s, c := serverTLS(cert), clientTLS()
	for _, cfg := range []*tls.Config{s, c} {
		if cfg.MinVersion != tls.VersionTLS13 || cfg.MaxVersion != tls.VersionTLS13 {
			t.Fatal("not TLS 1.3 only")
		}
		if len(cfg.NextProtos) != 1 || cfg.NextProtos[0] != ALPN {
			t.Fatalf("ALPN %v", cfg.NextProtos)
		}
	}
	if !s.SessionTicketsDisabled {
		t.Fatal("session tickets enabled")
	}
	if !c.InsecureSkipVerify || c.VerifyPeerCertificate == nil || c.ClientSessionCache != nil {
		t.Fatal("client must check only the certificate shape and never resume")
	}
}

func TestNewCertificateIsEphemeral(t *testing.T) {
	a, err := newCertificate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := newCertificate()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a.Certificate[0], b.Certificate[0]) {
		t.Fatal("two certificates are equal")
	}
	if _, ok := a.PrivateKey.(ed25519.PrivateKey); !ok {
		t.Fatalf("key type %T", a.PrivateKey)
	}
	if err := verifyPeerCertificate(a.Certificate, nil); err != nil {
		t.Fatal(err)
	}
}

func signedBy(t *testing.T, parent *x509.Certificate, parentKey ed25519.PrivateKey) ([]byte, *x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	if parent == nil {
		parent, parentKey = tmpl, priv
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return der, cert, priv
}

func TestVerifyPeerCertificate(t *testing.T) {
	caDER, ca, caKey := signedBy(t, nil, nil)
	leafDER, _, _ := signedBy(t, ca, caKey)
	for name, raw := range map[string][][]byte{
		"none":            nil,
		"empty":           {{}},
		"garbage":         {[]byte("not a certificate")},
		"chain":           {leafDER, caDER},
		"not self-signed": {leafDER},
	} {
		if err := verifyPeerCertificate(raw, nil); !errors.Is(err, errPeerCert) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	if err := verifyPeerCertificate([][]byte{caDER}, nil); err != nil {
		t.Fatalf("self-signed: %v", err)
	}
}

func TestDialRejectsChainedCertificate(t *testing.T) {
	checkGoroutines(t)
	_, ca, caKey := signedBy(t, nil, nil)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, pub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der, ca.Raw}, PrivateKey: priv}
	ln, err := quic.ListenAddr("127.0.0.1:0", serverTLS(cert), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, err = Dial(testCtx(t), ln.Addr().String(), nil)
	if !errors.Is(err, ErrHandshake) {
		t.Fatalf("got %v, want ErrHandshake", err)
	}
}

func TestALPNMismatch(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	other := clientTLS()
	other.NextProtos = []string{"other/0"}
	if c, err := rawTransport(t).Dial(testCtx(t), udpAddr(t, l.Addr().String()), other, nil); err == nil {
		c.CloseWithError(0, "")
		t.Fatal("listener accepted another ALPN")
	}

	cert, err := newCertificate()
	if err != nil {
		t.Fatal(err)
	}
	srv := serverTLS(cert)
	srv.NextProtos = []string{"other/0"}
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if _, err := Dial(testCtx(t), ln.Addr().String(), nil); !errors.Is(err, ErrHandshake) {
		t.Fatalf("got %v, want ErrHandshake", err)
	}
}

func TestExportKey(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	c1 := dial(t, l.Addr().String(), nil)
	s1 := accept(t, l)
	c2 := dial(t, l.Addr().String(), nil)
	accept(t, l)

	const label = "kolo-test/session/v0"
	kc, err := c1.ExportKey(label)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := s1.ExportKey(label)
	if err != nil {
		t.Fatal(err)
	}
	if kc != ks {
		t.Fatal("ends of one connection disagree")
	}
	if other, _ := c1.ExportKey("kolo-test/other/v0"); other == kc {
		t.Fatal("labels give the same key")
	}
	if k2, _ := c2.ExportKey(label); k2 == kc {
		t.Fatal("two connections give the same key")
	}
	if _, err := c1.ExportKey(""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty label: %v", err)
	}
}

func TestRelayInTheMiddleChangesExportKey(t *testing.T) {
	checkGoroutines(t)
	ctx := testCtx(t)
	target := listen(t, nil)
	relay := listen(t, nil)
	relayed := make(chan struct{})
	go func() {
		defer close(relayed)
		down, err := relay.Accept(ctx)
		if err != nil {
			return
		}
		up, err := Dial(ctx, target.Addr().String(), nil)
		if err != nil {
			return
		}
		defer up.Close()
		ds, err := down.AcceptStream(ctx)
		if err != nil {
			return
		}
		us, err := up.OpenStream(ctx)
		if err != nil {
			return
		}
		go func() {
			io.Copy(us, ds)
			us.Close()
		}()
		io.Copy(ds, us)
		ds.Close()
	}()

	client := dial(t, relay.Addr().String(), nil)
	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	cs.Close()
	server := accept(t, target)
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const label = "kolo-test/session/v0"
	kc, err := client.ExportKey(label)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := server.ExportKey(label)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(ss); err != nil || string(got) != "hello" {
		t.Fatalf("server read %q, %v", got, err)
	}
	ss.Write([]byte("hi"))
	ss.Close()
	if got, err := io.ReadAll(cs); err != nil || string(got) != "hi" {
		t.Fatalf("client read %q, %v", got, err)
	}
	<-relayed
	if kc == ks {
		t.Fatal("both ends agree through a relay")
	}
}
