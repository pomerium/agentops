package agenticrun

import "fmt"

type Executor struct {
	Namespace      string
	ServiceAccount string
	PodName        string
	PodUID         string
}

func (e Executor) Seal() map[string]string {
	return map[string]string{
		"kubernetes.io.namespace":           e.Namespace,
		"kubernetes.io.serviceaccount.name": e.ServiceAccount,
		"kubernetes.io.pod.name":            e.PodName,
		"kubernetes.io.pod.uid":             e.PodUID,
	}
}

func (e Executor) Validate() error {
	switch {
	case e.Namespace == "":
		return fmt.Errorf("executor namespace is empty")
	case e.ServiceAccount == "":
		return fmt.Errorf("executor service account is empty")
	case e.PodName == "":
		return fmt.Errorf("executor pod name is empty")
	case e.PodUID == "":
		return fmt.Errorf("executor pod uid is empty")
	}
	return nil
}
