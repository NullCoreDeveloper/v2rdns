//go:build android

package logger

/*
#cgo LDFLAGS: -llog
#include <android/log.h>
#include <stdlib.h>
*/
import "C"
import "unsafe"

func printAndroidLog(level int, appName, msg string) {
	var prio C.int
	switch level {
	case levelDebug:
		prio = C.ANDROID_LOG_DEBUG
	case levelInfo:
		prio = C.ANDROID_LOG_INFO
	case levelWarn:
		prio = C.ANDROID_LOG_WARN
	case levelError:
		prio = C.ANDROID_LOG_ERROR
	default:
		prio = C.ANDROID_LOG_INFO
	}

	cAppName := C.CString(appName)
	cMsg := C.CString(msg)
	
	C.__android_log_write(prio, cAppName, cMsg)
	
	C.free(unsafe.Pointer(cAppName))
	C.free(unsafe.Pointer(cMsg))
}
