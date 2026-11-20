// asyncctl performs explicit, bounded dead-letter administration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lwbio/async/internal/maintenance"
	"github.com/lwbio/async/internal/protocol"
)

func run(ctx context.Context, args []string, url string, stdout, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "inspect" && args[0] != "replay") {
		return errors.New("usage: asyncctl inspect|replay --namespace NAME --kind request|event|result --identity NAME [options]; broker URL comes from AMQP_URL")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	var o maintenance.Options
	var kind, auditPath string
	var timeout time.Duration
	flags.StringVar(&o.Namespace, "namespace", "", "runtime namespace")
	flags.StringVar(&kind, "kind", "", "request, event or result")
	flags.StringVar(&o.Identity, "identity", "", "request operation or stable event/result subscription identity")
	flags.IntVar(&o.Limit, "limit", 20, "maximum messages to scan (1-1000), including filtered/invalid messages")
	flags.StringVar(&o.Reason, "reason", "", "only select this dead-letter reason")
	flags.Float64Var(&o.Rate, "rate", 10, "maximum replay rate in messages/s")
	flags.BoolVar(&o.Apply, "apply", false, "actually replay; default is inspection only")
	flags.BoolVar(&o.ResetRetries, "reset-retries", false, "explicitly reset business Attempt to zero")
	flags.StringVar(&o.Actor, "actor", "", "operator identity, required with --apply")
	flags.StringVar(&auditPath, "audit", "", "append-only JSONL audit file, required with --apply")
	flags.DurationVar(&timeout, "timeout", 30*time.Second, "overall operation deadline")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || timeout <= 0 {
		return errors.New("unexpected positional arguments or invalid timeout")
	}
	if args[0] == "inspect" && o.Apply {
		return errors.New("inspect cannot use --apply")
	}
	if o.Apply && (auditPath == "" || o.Actor == "") {
		return errors.New("--apply requires --audit FILE and --actor NAME")
	}
	if url == "" {
		return errors.New("set AMQP_URL; do not pass broker credentials on the command line")
	}
	o.Kind = protocol.Kind(kind)
	var audit io.Writer
	if o.Apply {
		file, err := os.OpenFile(auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return errors.New("could not open audit file")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return errors.New("audit file must be a regular private file (mode 0600)")
		}
		audit = file
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	report, err := maintenance.Run(ctx, url, o, audit)
	// Emit partial progress even when an operation fails; never emit the URL.
	if encodeErr := json.NewEncoder(stdout).Encode(report); encodeErr != nil {
		return errors.Join(err, errors.New("write report failed"))
	}
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv("AMQP_URL"), os.Stdout, os.Stderr)
	stop()
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
