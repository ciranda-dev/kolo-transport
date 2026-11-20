package kolotransport

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// The files below are embedded, so the test reads nothing at run time.
var (
	//go:embed internal/quicgo
	vendoredQuicGo embed.FS
	//go:embed scripts/vendor-quicgo.sh
	vendorScript string
	//go:embed go.mod
	goMod string
	//go:embed THIRD_PARTY_NOTICES
	notices string
	//go:embed internal/bbr/LICENSE.md
	bbrLicense string
)

const reVendor = "run scripts/vendor-quicgo.sh (see the maintenance section of docs/guide.md)"

// The files of internal/quicgo agree with each other and with the pin of the script: a hand edit that leaves CHECKSUMS alone,
// a half-finished re-vendor or a bumped pin without a re-vendor fails here. This cannot tell an edit that also rewrites
// CHECKSUMS; TestVendoredQuicGoIsUpstreamPlusPatch can.
func TestVendoredQuicGoMatchesThePin(t *testing.T) {
	m := regexp.MustCompile(`(?m)^pin=(v\d+\.\d+\.\d+)$`).FindStringSubmatch(vendorScript)
	if m == nil {
		t.Fatal("scripts/vendor-quicgo.sh has no pin=vX.Y.Z line")
	}
	read := func(name string) string {
		b, err := vendoredQuicGo.ReadFile("internal/quicgo/" + name)
		if err != nil {
			t.Fatalf("%v: %s", err, reVendor)
		}
		return string(b)
	}
	version, sum, _ := strings.Cut(strings.TrimSpace(read("VERSION")), "\n")
	if version != m[1] {
		t.Fatalf("internal/quicgo is %s but the script pins %s: %s", version, m[1], reVendor)
	}
	if !strings.HasPrefix(sum, "h1:") {
		t.Fatalf("internal/quicgo/VERSION has no module checksum: %s", reVendor)
	}
	if ps := regexp.MustCompile(`(?m)^pinsum=(h1:\S+)$`).FindStringSubmatch(vendorScript); ps == nil || ps[1] != sum {
		t.Fatalf("internal/quicgo/VERSION has module checksum %s, the script pins another one (pinsum=): %s", sum, reVendor)
	}

	want := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(read("CHECKSUMS")), "\n") {
		hash, name, ok := strings.Cut(line, "  ")
		if !ok {
			t.Fatalf("bad CHECKSUMS line %q: %s", line, reVendor)
		}
		want["internal/quicgo/"+strings.TrimPrefix(name, "./")] = hash
	}
	seen := 0
	err := fs.WalkDir(vendoredQuicGo, "internal/quicgo", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || name == "internal/quicgo/CHECKSUMS" {
			return err
		}
		b, err := vendoredQuicGo.ReadFile(name)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		switch hash, ok := want[name]; {
		case !ok:
			t.Errorf("%s is not in internal/quicgo/CHECKSUMS: %s", name, reVendor)
		case hash != hex.EncodeToString(sum[:]):
			t.Errorf("%s differs from the vendored copy: %s", name, reVendor)
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != len(want) {
		t.Errorf("internal/quicgo has %d files, CHECKSUMS lists %d: %s", seen, len(want), reVendor)
	}

	err = fs.WalkDir(vendoredQuicGo, "internal/quicgo", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(name, ".go") {
			return err
		}
		b, _ := vendoredQuicGo.ReadFile(name)
		if strings.Contains(string(b), "\"github.com/quic-go/quic-go") {
			t.Errorf("%s imports the upstream path: %s", name, reVendor)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The vendored tree is the pinned upstream release plus PATCH: the test runs `vendor-quicgo.sh verify` on a copy of the
// embedded tree, which downloads nothing (the module must be in the module cache, else the test skips) and compares the
// module sum of the release with the one the script pins.
func TestVendoredQuicGoIsUpstreamPlusPatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the vendor script needs a POSIX shell")
	}
	for _, tool := range []string{"sh", "go", "patch", "diff"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed: run scripts/vendor-quicgo.sh verify instead", tool)
		}
	}
	root := t.TempDir()
	write := func(name, content string) {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("scripts/vendor-quicgo.sh", vendorScript)
	err := fs.WalkDir(vendoredQuicGo, "internal/quicgo", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := vendoredQuicGo.ReadFile(name)
		write(name, string(b))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	verify := func() (string, error) {
		cmd := exec.Command("sh", filepath.Join(root, "scripts", "vendor-quicgo.sh"), "verify")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod", "GOPROXY=off", "GOSUMDB=off")
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	out, err := verify()
	if err != nil && strings.Contains(out, "cannot download") {
		t.Skipf("the pinned quic-go is not in the module cache (run scripts/vendor-quicgo.sh verify with the network): %s", out)
	}
	if err != nil {
		t.Fatalf("internal/quicgo is not the pinned release plus PATCH: %v\n%s", err, out)
	}

	// An edit that also rewrites its line of CHECKSUMS passes the other test and must fail here.
	p := filepath.Join(root, "internal", "quicgo", "sys_conn.go")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, append(b, "\n// edited\n"...), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := verify(); err == nil || !strings.Contains(out, "sys_conn.go") {
		t.Fatalf("an edited vendored file was not detected: %v\n%s", err, out)
	}
}

// go.mod depends on no copy of quic-go and replaces nothing: the vendored tree is the only one.
func TestGoModHasNoQuicGoAndNoReplace(t *testing.T) {
	if strings.Contains(goMod, "quic-go") || strings.Contains(goMod, "replace") {
		t.Fatalf("go.mod must not mention quic-go or replace:\n%s", goMod)
	}
}

// The licenses of the vendored code travel with it, as the MIT license requires.
func TestThirdPartyNoticesKeepTheLicenses(t *testing.T) {
	license, err := vendoredQuicGo.ReadFile("internal/quicgo/LICENSE")
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"internal/quicgo/LICENSE": string(license), "internal/bbr/LICENSE.md": bbrLicense} {
		line, _, _ := strings.Cut(strings.TrimPrefix(text, "MIT License\n\n"), "\n")
		if !strings.HasPrefix(line, "Copyright ") || !strings.Contains(text, "Permission is hereby granted, free of charge") {
			t.Fatalf("%s lost its copyright line or permission notice", name)
		}
		if !strings.Contains(notices, line) || !strings.Contains(notices, "The above copyright notice and this permission notice shall be included in all") {
			t.Errorf("THIRD_PARTY_NOTICES does not carry %q from %s", line, name)
		}
	}
	for _, want := range []string{"Copyright 2015 The Chromium Authors", "Redistributions of source code must retain the above copyright", "Neither the name of Google Inc."} {
		if !strings.Contains(notices, want) {
			t.Errorf("THIRD_PARTY_NOTICES lost the BSD notice of the BBR port: %q", want)
		}
	}
}
