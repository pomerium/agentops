// Command sidecarprobe drives a locally running sidecar container through the
// production control protocol for fast debugging without Kubernetes:
//
//	docker network create probe
//	docker run -d --rm --name probe-echo --network probe mendhak/http-https-echo:36
//	docker run -d --rm --name probe-sidecar --network probe \
//	  -e SIDECAR_HTTP_TEST_PORT=9999 \
//	  -e SIDECAR_HTTP_TEST_UPSTREAM_URL=http://probe-echo:8080 \
//	  -e SIDECAR_HTTP_TEST_HEADER_X_API_KEY=dummy agentops-sidecar:dev
//	go run ./internal/e2e/cmd/sidecarprobe probe-sidecar
//
// It execs `sidecar serve` in the container, sends a Configure, waits for the
// status, and leaves the stream open until killed.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	sidecarpb "github.com/pomerium/agentops/internal/sidecar/pb"
	"github.com/pomerium/agentops/internal/sidecar/stdiorpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: %s <container> [upstream-url]", os.Args[0])
	}
	container := os.Args[1]
	upstream := "http://probe-echo:8080/mcp"
	if len(os.Args) > 2 {
		upstream = os.Args[2]
	}

	cmd := exec.Command("docker", "exec", "-i", container, "/usr/local/bin/sidecar", "serve")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	cc, err := stdiorpc.Dial(stdiorpc.NewConn(stdout, stdin, nil))
	if err != nil {
		return err
	}
	defer cc.Close()

	stream, err := sidecarpb.NewSidecarControlServiceClient(cc).Control(context.Background())
	if err != nil {
		return err
	}
	err = stream.Send(&sidecarpb.ControlRequest{
		Msg: &sidecarpb.ControlRequest_Configure{Configure: &sidecarpb.Configure{
			Endpoints: []*sidecarpb.HttpEndpoint{{
				Name:        "probe",
				ListenPort:  9100,
				UpstreamUrl: upstream,
				Headers:     []*sidecarpb.Header{{Name: "authorization", Value: "Bearer probe-token"}},
			}},
		}},
	})
	if err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	fmt.Printf("status: %v\n", resp.GetStatus())
	fmt.Println("control stream open; ctrl-c to exit")
	_, err = stream.Recv() // block until the sidecar ends the session
	return err
}
