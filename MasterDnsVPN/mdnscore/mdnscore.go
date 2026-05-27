// ==============================================================================
// MasterDNSVPN Public Android Bindings (mdnscore)
// ==============================================================================

package mdnscore

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"masterdnsvpn-go/internal/client"
	"masterdnsvpn-go/internal/config"
)

var (
	mdnsClient *client.Client
	mdnsCancel context.CancelFunc
	mdnsMu     sync.Mutex
)

// StartMdnsClient starts the MasterDNSVPN client in the background with a base64-encoded JSON config.
func StartMdnsClient(configJsonBase64 string, logPath string, configDir string) error {
	mdnsMu.Lock()
	defer mdnsMu.Unlock()

	if mdnsClient != nil {
		return fmt.Errorf("MasterDNSVPN client is already running")
	}

	// Load config from base64 JSON
	cfg, err := config.LoadClientConfigFromJSONBase64(configJsonBase64)
	if err != nil {
		return fmt.Errorf("failed to load client config: %w", err)
	}
	
	if configDir != "" {
		cfg.ConfigDir = configDir
	}

	// Bootstrap client
	c, err := client.BootstrapLoadedConfig(cfg, logPath)
	if err != nil {
		return fmt.Errorf("failed to bootstrap client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mdnsCancel = cancel
	mdnsClient = c

	// Run in a separate goroutine
	go func() {
		defer func() {
			mdnsMu.Lock()
			mdnsClient = nil
			mdnsCancel = nil
			mdnsMu.Unlock()
		}()

		log.Println("Starting MasterDNSVPN client runner goroutine")
		if err := c.Run(ctx); err != nil {
			log.Printf("MasterDNSVPN client run failed: %v", err)
		}
		log.Println("MasterDNSVPN client runner goroutine stopped")
	}()

	return nil
}

// StopMdnsClient stops the running MasterDNSVPN client.
func StopMdnsClient() error {
	mdnsMu.Lock()
	defer mdnsMu.Unlock()

	if mdnsClient == nil {
		return nil // Not running
	}

	if mdnsCancel != nil {
		mdnsCancel()
	}

	// Wait up to 5 seconds for it to stop
	for i := 0; i < 50; i++ {
		if mdnsClient == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// IsMdnsClientRunning returns whether the client is currently running.
func IsMdnsClientRunning() bool {
	mdnsMu.Lock()
	defer mdnsMu.Unlock()
	return mdnsClient != nil
}
