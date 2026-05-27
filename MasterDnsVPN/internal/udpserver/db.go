package udpserver

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// getClientKeyAndCheckStatus queries the 3x-ui SQLite database to retrieve the client's UUID/key
// and verifies that the client is active, has not exceeded their traffic limit, and has not expired.
func getClientKeyAndCheckStatus(dbPath string, email string) (string, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return "", fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	var uuid sql.NullString
	var password sql.NullString
	var enable int
	var up int64
	var down int64
	var total int64
	var expiryTime int64

	// We look up the client by email from clients table and join with client_traffics
	query := `
		SELECT 
			c.uuid, 
			c.password, 
			ct.enable, 
			ct.up, 
			ct.down, 
			ct.total, 
			ct.expiry_time 
		FROM clients c
		LEFT JOIN client_traffics ct ON ct.email = c.email
		WHERE c.email = ?
		LIMIT 1
	`
	err = db.QueryRow(query, email).Scan(&uuid, &password, &enable, &up, &down, &total, &expiryTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("client %s not found in database", email)
		}
		return "", fmt.Errorf("database query failed: %w", err)
	}

	// 1. Check if client is enabled
	if enable != 1 {
		return "", fmt.Errorf("client %s is disabled", email)
	}

	// 2. Check traffic limits (if total > 0)
	if total > 0 && (up+down) >= total {
		return "", fmt.Errorf("client %s has exceeded traffic limit (%d / %d)", email, up+down, total)
	}

	// 3. Check subscription expiry (if expiryTime > 0)
	// expiryTime in 3x-ui is stored in milliseconds
	if expiryTime > 0 {
		nowMs := time.Now().UnixMilli()
		if nowMs >= expiryTime {
			return "", fmt.Errorf("client %s subscription has expired (expired at: %s)", email, time.UnixMilli(expiryTime).Format(time.RFC3339))
		}
	}

	// Use UUID if present, otherwise fall back to password
	clientKey := uuid.String
	if clientKey == "" {
		clientKey = password.String
	}

	if clientKey == "" {
		return "", fmt.Errorf("client %s has empty key/uuid", email)
	}

	return clientKey, nil
}
