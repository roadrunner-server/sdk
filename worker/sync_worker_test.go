package worker

import (
	"os/exec"
	"testing"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NotStarted_String(t *testing.T) {
	cmd := exec.Command("php", "tests/client.php", "echo", "pipes")

	w, _ := InitBaseWorker(cmd)
	assert.Contains(t, w.String(), "php tests/client.php echo pipes")
	assert.Contains(t, w.String(), "inactive")
	assert.Contains(t, w.String(), "num_execs: 0")
}

func Test_NotStarted_Exec(t *testing.T) {
	cmd := exec.Command("php", "tests/client.php", "echo", "pipes")

	w, err := InitBaseWorker(cmd)
	require.NoError(t, err)
	sw := From(w)

	res, err := sw.Exec(&payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)

	assert.Contains(t, err.Error(), "Process is not ready (inactive)")
}
