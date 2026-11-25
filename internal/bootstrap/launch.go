package bootstrap

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// RunnerCommand selects the native entrypoint and an explicit worker environment.
// The runner consumes/removes ACTIONS_RUNNER_INPUT_JITCONFIG itself. It writes
// credential files inside this VM; physical VM destruction remains mandatory.
// Neither bootstrap nor unrelated inherited credentials reach the runner.
func RunnerCommand(root string, payload []byte, environment []string) (string, []string, []string, error) {
	executable, err := ValidateRunnerRoot(root)
	if err != nil {
		return "", nil, nil, err
	}
	if !ValidJIT(payload) {
		return "", nil, nil, errors.New("invalid JIT payload")
	}
	allowed := map[string]bool{"HOME": true, "USER": true, "LOGNAME": true, "PATH": true,
		"TMPDIR": true, "LANG": true, "LC_ALL": true, "TZ": true, "TERM": true, "DEVELOPER_DIR": true}
	var env []string
	seen := map[string]bool{}
	for _, item := range environment {
		name, _, ok := strings.Cut(item, "=")
		if ok && allowed[name] && !seen[name] {
			env = append(env, item)
			seen[name] = true
		}
	}
	if !seen["HOME"] || !seen["PATH"] {
		return "", nil, nil, errors.New("worker HOME and PATH are required")
	}
	env = append(env, "ACTIONS_RUNNER_INPUT_JITCONFIG="+string(payload))
	size := len(executable) + len("run") + 2
	for _, item := range env {
		size += len(item) + 1
	}
	if size > 96<<10 {
		return "", nil, nil, errors.New("runner environment exceeds execution budget")
	}
	return executable, []string{executable, "run"}, env, nil
}

// ValidateRunnerRoot also runs before fetching a one-use capability. The native
// listener loads .env before CommandSettings and may overwrite JIT, so a fresh
// pool image must not contain .env or any prior registration configuration.
func ValidateRunnerRoot(root string) (string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", errors.New("runner root must be absolute and clean")
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return "", errors.New("runner root must be a directory, not a symlink")
	}
	for _, name := range []string{".env", ".runner", ".credentials", ".credentials_rsaparams", ".service"} {
		_, err := os.Lstat(filepath.Join(root, name))
		if !errors.Is(err, os.ErrNotExist) {
			return "", errors.New("runner root contains prior configuration or cannot be inspected")
		}
	}
	bin := filepath.Join(root, "bin")
	binInfo, err := os.Lstat(bin)
	if err != nil || !binInfo.IsDir() || binInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("runner bin directory must be a real directory")
	}
	executable := filepath.Join(root, "bin", "Runner.Listener")
	info, err = os.Lstat(executable)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return "", errors.New("native runner entrypoint is absent or not executable")
	}
	return executable, nil
}
