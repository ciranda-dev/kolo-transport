package kolotransport

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"
)

// ALPN is the TLS application protocol both ends must offer.
const ALPN = "kolo/0"

var errPeerCert = errors.New("kolotransport: peer certificate is not one self-signed certificate")

func newCertificate() (tls.Certificate, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}, nil
}

func serverTLS(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		MinVersion:             tls.VersionTLS13,
		MaxVersion:             tls.VersionTLS13,
		Certificates:           []tls.Certificate{cert},
		NextProtos:             []string{ALPN},
		SessionTicketsDisabled: true,
	}
}

func clientTLS() *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		MaxVersion:            tls.VersionTLS13,
		NextProtos:            []string{ALPN},
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verifyPeerCertificate,
	}
}

// verifyPeerCertificate checks the shape only: the certificate carries no identity.
func verifyPeerCertificate(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) != 1 {
		return errPeerCert
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return errPeerCert
	}
	if cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature) != nil {
		return errPeerCert
	}
	return nil
}
