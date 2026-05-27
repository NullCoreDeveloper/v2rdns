// ==============================================================================
// MasterDNSVPN Integration for libv2ray
// ==============================================================================

package libv2ray

import (
	"masterdnsvpn-go/mdnscore"
)

// StartMdnsClient starts the MasterDNSVPN client in the background with a base64-encoded JSON config.
func StartMdnsClient(configJsonBase64 string, logPath string, configDir string) error {
	return mdnscore.StartMdnsClient(configJsonBase64, logPath, configDir)
}

// StopMdnsClient stops the running MasterDNSVPN client.
func StopMdnsClient() error {
	return mdnscore.StopMdnsClient()
}

// IsMdnsClientRunning returns whether the client is currently running.
func IsMdnsClientRunning() bool {
	return mdnscore.IsMdnsClientRunning()
}
