/*
Copyright 2026 The Kaalm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testPKI is one CA with helpers to issue PEM-encoded leaves.
type testPKI struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	caPEM []byte
}

func newTestPKI(t *testing.T, cn string) *testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testPKI{cert: cert, key: key,
		caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issuePEM signs a leaf for the given SANs, usable for both server and
// client auth, and returns the certificate and key PEM.
func (p *testPKI) issuePEM(t *testing.T, sans ...string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: sans[0]},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    sans,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.cert, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// writeRotated writes content and bumps the file's mtime forward, so the
// loader's mtime comparison always observes a change even on coarse clocks.
func writeRotated(t *testing.T, path string, content []byte, bump time.Duration) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Add(bump)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

// TestDialTLSContext_ReloadsPerDial proves the #149 contract for the client
// half: new connections pick up rotated client certificates and rotated CA
// bundles without rebuilding the http.Client.
func TestDialTLSContext_ReloadsPerDial(t *testing.T) {
	caA := newTestPKI(t, "ca-a")
	caB := newTestPKI(t, "ca-b")
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "ca.crt")

	// Server: CA-A-signed serving cert, but it verifies CLIENTS against CA-B.
	serverCert, serverKey := caA.issuePEM(t, "svc.test")
	serverPair, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(caB.cert)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverPair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	// Client files: a CA-A-signed client cert (the server will reject it)
	// and CA-A as trust (the server cert verifies).
	certA, keyA := caA.issuePEM(t, "client.test")
	writeRotated(t, certFile, certA, 0)
	writeRotated(t, keyFile, keyA, 0)
	writeRotated(t, caFile, caA.caPEM, 0)

	loader := &CertLoader{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	transport := &http.Transport{DialTLSContext: loader.DialTLSContext("svc.test")}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	url := "https://" + ln.Addr().String() + "/"

	if _, err := client.Get(url); err == nil {
		t.Fatal("a CA-A client cert must be rejected by a CA-B-verifying server")
	}

	// Rotate the client pair to CA-B-signed: the next dial must present it.
	certB, keyB := caB.issuePEM(t, "client.test")
	writeRotated(t, keyFile, keyB, 2*time.Second)
	writeRotated(t, certFile, certB, 2*time.Second)
	transport.CloseIdleConnections()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("rotated client cert was not picked up: %v", err)
	}
	_ = resp.Body.Close()

	// Rotate the trust bundle to CA-B only: the CA-A server cert must now
	// fail verification on the next new connection.
	writeRotated(t, caFile, caB.caPEM, 4*time.Second)
	transport.CloseIdleConnections()
	if _, err := client.Get(url); err == nil {
		t.Fatal("rotated CA bundle was not picked up: the CA-A server cert still verified")
	}
}

// TestServerMTLSConfig_RebuildsClientCAs proves the #149 contract for the
// listener half: the ClientCAs pool handed to a new connection reflects the
// bundle on disk, and the serving cert resolves through the loader.
func TestServerMTLSConfig_RebuildsClientCAs(t *testing.T) {
	caA := newTestPKI(t, "ca-a")
	caB := newTestPKI(t, "ca-b")
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "ca.crt")
	certPEM, keyPEM := caA.issuePEM(t, "listener.test")
	writeRotated(t, certFile, certPEM, 0)
	writeRotated(t, keyFile, keyPEM, 0)
	writeRotated(t, caFile, caA.caPEM, 0)

	loader := &CertLoader{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	cfg, err := loader.ServerMTLSConfig(tls.VerifyClientCertIfGiven)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.VerifyClientCertIfGiven || cfg.ClientCAs == nil {
		t.Fatalf("initial config wrong: %+v", cfg)
	}
	if cert, err := cfg.GetCertificate(nil); err != nil || cert == nil {
		t.Fatalf("GetCertificate = %v, %v", cert, err)
	}

	before := cfg.ClientCAs
	writeRotated(t, caFile, append(append([]byte{}, caA.caPEM...), caB.caPEM...), 2*time.Second)
	rotated, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.ClientCAs.Equal(before) {
		t.Error("GetConfigForClient did not rebuild ClientCAs from the rotated bundle")
	}
	if rotated.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Errorf("rotated config lost the client-auth mode: %v", rotated.ClientAuth)
	}
	if cert, err := rotated.GetCertificate(nil); err != nil || cert == nil {
		t.Fatalf("rotated GetCertificate = %v, %v", cert, err)
	}
}
