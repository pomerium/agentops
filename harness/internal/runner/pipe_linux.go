package runner

import "golang.org/x/sys/unix"

func pipeBuffered(fd int) (int, error) { return unix.IoctlGetInt(fd, unix.TIOCINQ) }
