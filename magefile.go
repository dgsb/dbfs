//go:build mage

package main

import (
	"github.com/magefile/mage/sh"
)

func Build() error {
	return sh.Run("go", "build", "./")
}

func Test() error {
	return sh.Run("go", "test", "./...")
}

func Bench() error {
	return sh.Run("go", "test", "-bench", ".", "-run", "^$", "./...")
}
