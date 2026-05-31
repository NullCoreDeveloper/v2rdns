//go:build !android || (android && !cgo)

package logger

func printAndroidLog(level int, appName, msg string) {
	// No-op for non-Android platforms
}
