package common

import (
	"os"

	"golang.org/x/term"
)

type TerminalState = term.State

func IsTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) || term.IsTerminal(int(os.Stdout.Fd()))
}

func MakeRaw() (*TerminalState, error) {
	return term.MakeRaw(int(os.Stdin.Fd()))
}

func RestoreTerminal(state *TerminalState) error {
	if state == nil {
		return nil
	}
	return term.Restore(int(os.Stdin.Fd()), state)
}

func GetTerminalSize() ([2]int, error) {
	cols, rows, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		cols, rows, err = term.GetSize(int(os.Stdin.Fd()))
	}
	return [2]int{cols, rows}, err
}
