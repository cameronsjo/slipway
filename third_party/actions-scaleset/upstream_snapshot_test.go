package scaleset

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
)

// These files are the unmodified production surface copied from the reviewed
// upstream commit. Any upstream-source edit requires an explicit pin review;
// the local extension lives in resume.go.
var pinnedUpstreamSHA256 = map[string]string{
	"LICENSE":           "2510b446bc1f0cf9702453075d20cd88631e20e5642658edb7325d9c1eb534f7",
	"client.go":         "e45ee6f59ffe344c5a610b898d85f819a37b8a48ca09de70ecaab65b64d9d431",
	"common_client.go":  "7551f1f6a1311bea0a915889ee4c7c2433cd609c36f76bc24215109119fb694e",
	"config.go":         "0523a5302fdcba40aa681fedc906a4f69f4d4ccfcf55127ff96d467637ca3173",
	"errors.go":         "a2c00be419c4b9b367a066ca11e9e7abbec996a81f9d6f7a4e5b777dd2ad0258",
	"jwt_provider.go":   "1e4ac12f3a45c991357b4feaf0d7108bc1bd7e0fa601b22483c5b7a908588017",
	"session_client.go": "8ea84d21c0e8bf6de19c869d4f46b774403bcd5b5a1e95824ffd6794e2336109",
	"types.go":          "66cbf915fe9251ba28c2effa6f72d1c4155679720b696f838d77e6af144f73c5",
}

func verifyPinnedUpstream(readFile func(string) ([]byte, error)) error {
	for name, expected := range pinnedUpstreamSHA256 {
		data, err := readFile(name)
		if err != nil {
			return err
		}
		if actual := fmt.Sprintf("%x", sha256.Sum256(data)); actual != expected {
			return fmt.Errorf("%s differs from pinned upstream source: %s", name, actual)
		}
	}
	return nil
}

func TestPinnedUpstreamSnapshot(t *testing.T) {
	if err := verifyPinnedUpstream(os.ReadFile); err != nil {
		t.Fatal(err)
	}
	if err := verifyPinnedUpstream(func(name string) ([]byte, error) {
		data, err := os.ReadFile(name)
		if name == "client.go" {
			data = append(data, '\n')
		}
		return data, err
	}); err == nil {
		t.Fatal("snapshot check accepted changed upstream source")
	}
}
