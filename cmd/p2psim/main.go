package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/net/context"

	"github.com/chzyer/readline"
	"github.com/ethereum/go-ethereum/p2p/simulations"
)

const prompt = "\033[31mp2psim»\033[0m "

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	l, err := readline.NewEx(&readline.Config{
		Prompt: prompt,
	})
	if err != nil {
		return err
	}
	defer l.Close()

	sim := simulations.NewSimulator(l.Stdout())
	for {
		line, err := l.Readline()
		if err != nil {
			switch err {
			case readline.ErrInterrupt:
				if len(line) == 0 {
					return nil
				} else {
					continue
				}
			case io.EOF:
				return nil
			default:
				return err
			}
		}

		ctx := context.Background()
		if err := sim.RunCommand(ctx, strings.TrimSpace(line)); err != nil {
			if err == simulations.ErrExit {
				fmt.Fprintln(l.Stdout(), "bye!")
				return nil
			}
			fmt.Fprintln(l.Stderr(), "ERROR:", err)
		}
	}
}
