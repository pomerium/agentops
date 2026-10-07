package agenticrun

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecutorSeal(t *testing.T) {
	t.Parallel()
	e := Executor{Namespace: "ns", ServiceAccount: "sa", PodName: "p", PodUID: "u"}
	assert.Equal(t, map[string]string{
		"kubernetes.io.namespace":           "ns",
		"kubernetes.io.serviceaccount.name": "sa",
		"kubernetes.io.pod.name":            "p",
		"kubernetes.io.pod.uid":             "u",
	}, e.Seal())
	assert.NoError(t, e.Validate())
	assert.Error(t, Executor{Namespace: "ns"}.Validate())
}
