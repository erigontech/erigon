package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	_ "modernc.org/sqlite"
)

type DBSQLite struct {
	db *sql.DB
}

// language=SQL
const (
	sqlCreateSchema = `
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,

    ip TEXT,
    port_disc INTEGER,
    port_rlpx INTEGER,
    ip_v6 TEXT,
    ip_v6_port_disc INTEGER,
    ip_v6_port_rlpx INTEGER,
    addr_updated INTEGER NOT NULL,

	ping_try INTEGER NOT NULL DEFAULT 0,

    compat_fork INTEGER,
    compat_fork_updated INTEGER,

    client_id TEXT,
    network_id INTEGER,
    eth_version INTEGER,
    handshake_transient_err INTEGER NOT NULL DEFAULT 0,
    handshake_updated INTEGER,
    handshake_retry_time INTEGER,
    
    neighbor_keys TEXT,
    
    crawl_retry_time INTEGER
);

CREATE TABLE IF NOT EXISTS handshake_errors (
    id TEXT NOT NULL,
    err TEXT NOT NULL,
    updated INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sentry_candidates_intake (
    id INTEGER PRIMARY KEY,
    last_event_time INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_crawl_retry_time ON nodes (crawl_retry_time);
CREATE INDEX IF NOT EXISTS idx_nodes_ip ON nodes (ip);
CREATE INDEX IF NOT EXISTS idx_nodes_ip_v6 ON nodes (ip_v6);
CREATE INDEX IF NOT EXISTS idx_nodes_ping_try ON nodes (ping_try);
CREATE INDEX IF NOT EXISTS idx_nodes_compat_fork ON nodes (compat_fork);
CREATE INDEX IF NOT EXISTS idx_nodes_network_id ON nodes (network_id);
CREATE INDEX IF NOT EXISTS idx_nodes_handshake_retry_time ON nodes (handshake_retry_time);
CREATE INDEX IF NOT EXISTS idx_handshake_errors_id ON handshake_errors (id);
`

	sqlUpsertNodeAddr = `
INSERT INTO nodes(
	id,
    ip,
    port_disc,
    port_rlpx,
    ip_v6,
    ip_v6_port_disc,
    ip_v6_port_rlpx,
    addr_updated
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    ip = excluded.ip,
    port_disc = excluded.port_disc,
    port_rlpx = excluded.port_rlpx,
    ip_v6 = excluded.ip_v6,
    ip_v6_port_disc = excluded.ip_v6_port_disc,
    ip_v6_port_rlpx = excluded.ip_v6_port_rlpx,
    addr_updated = excluded.addr_updated
`

	sqlFindNodeAddr = `
SELECT
    ip,
    port_disc,
    port_rlpx,
    ip_v6,
    ip_v6_port_disc,
    ip_v6_port_rlpx
FROM nodes
WHERE id = ?
`

	sqlResetPingError = `
UPDATE nodes SET ping_try = 0 WHERE id = ?
`

	sqlUpdatePingError = `
UPDATE nodes SET ping_try = nodes.ping_try + 1 WHERE id = ?
`

	sqlCountPingErrors = `
SELECT ping_try FROM nodes WHERE id = ?
`

	sqlUpdateClientID = `
UPDATE nodes SET 
	client_id = ?, 
	handshake_updated = ?
WHERE id = ?
`

	sqlFindClientID = `
SELECT client_id FROM nodes WHERE id = ?
`

	sqlUpdateNetworkID = `
UPDATE nodes SET 
	network_id = ?, 
	handshake_updated = ?
WHERE id = ?
`

	sqlUpdateEthVersion = `
UPDATE nodes SET 
	eth_version = ?, 
	handshake_updated = ?
WHERE id = ?
`

	sqlUpdateHandshakeTransientError = `
UPDATE nodes SET 
	handshake_transient_err = ?, 
	handshake_updated = ?
WHERE id = ?
`

	sqlInsertHandshakeError = `
INSERT INTO handshake_errors(
	id,
	err,
	updated
) VALUES (?, ?, ?)
`

	sqlDeleteHandshakeErrors = `
DELETE FROM handshake_errors WHERE id = ?
`

	sqlFindHandshakeLastErrors = `
SELECT err, updated FROM handshake_errors
WHERE id = ?
ORDER BY updated DESC
LIMIT ?
`

	sqlUpdateHandshakeRetryTime = `
UPDATE nodes SET handshake_retry_time = ? WHERE id = ?
`

	sqlFindHandshakeRetryTime = `
SELECT handshake_retry_time FROM nodes WHERE id = ?
`

	sqlCountHandshakeCandidates = `
SELECT COUNT(*) FROM nodes
WHERE ((handshake_retry_time IS NULL) OR (handshake_retry_time < ?))
	AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
`

	sqlFindHandshakeCandidates = `
SELECT id FROM nodes
WHERE ((handshake_retry_time IS NULL) OR (handshake_retry_time < ?))
	AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
ORDER BY handshake_retry_time
LIMIT ?
`

	sqlMarkTakenHandshakeCandidates = `
UPDATE nodes SET handshake_retry_time = ? WHERE id IN (123)
`

	sqlUpdateForkCompatibility = `
UPDATE nodes SET compat_fork = ?, compat_fork_updated = ? WHERE id = ?
`

	sqlUpdateNeighborBucketKeys = `
UPDATE nodes SET neighbor_keys = ? WHERE id = ?
`

	sqlFindNeighborBucketKeys = `
SELECT neighbor_keys FROM nodes WHERE id = ?
`

	sqlUpdateSentryCandidatesLastEventTime = `
INSERT INTO sentry_candidates_intake(
	id,
	last_event_time
) VALUES (0, ?)
ON CONFLICT(id) DO UPDATE SET
	last_event_time = excluded.last_event_time
`

	sqlFindSentryCandidatesLastEventTime = `
SELECT last_event_time FROM sentry_candidates_intake WHERE id = 0
`

	sqlUpdateCrawlRetryTime = `
UPDATE nodes SET crawl_retry_time = ? WHERE id = ?
`

	sqlCountCandidates = `
SELECT COUNT(*) FROM nodes
WHERE ((crawl_retry_time IS NULL) OR (crawl_retry_time < ?))
	AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
`

	sqlFindCandidates = `
SELECT id FROM nodes
WHERE ((crawl_retry_time IS NULL) OR (crawl_retry_time < ?))
	AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
ORDER BY crawl_retry_time
LIMIT ?
`

	sqlMarkTakenNodes = `
UPDATE nodes SET crawl_retry_time = ? WHERE id IN (123)
`

	sqlCountNodes = `
SELECT COUNT(*) FROM nodes
WHERE (ping_try < ?)
    AND ((network_id = ?) OR (network_id IS NULL))
    AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
`

	sqlCountIPs = `
SELECT COUNT(DISTINCT ip) FROM nodes
WHERE (ping_try < ?)
    AND ((network_id = ?) OR (network_id IS NULL))
    AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
`

	sqlCountClients = `
SELECT COUNT(*) FROM nodes
WHERE (ping_try < ?)
    AND (network_id = ?)
    AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
	AND (client_id LIKE ?)
`

	sqlCountClientsWithNetworkID = `
SELECT COUNT(*) FROM nodes
WHERE (ping_try < ?)
    AND (network_id IS NOT NULL)
    AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
	AND (client_id LIKE ?)
`

	sqlCountClientsWithHandshakeTransientError = `
SELECT COUNT(*) FROM nodes
WHERE (ping_try < ?)
    AND (handshake_transient_err = 1)
    AND (network_id IS NULL)
    AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
	AND (client_id LIKE ?)
`

	sqlEnumerateClientIDs = `
SELECT client_id FROM nodes
WHERE (ping_try < ?)
    AND ((network_id = ?) OR (network_id IS NULL))
    AND ((compat_fork == TRUE) OR (compat_fork IS NULL))
`
)

func NewDBSQLite(filePath string) (*DBSQLite, error) {
	db, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB: %w", err)
	}

	_, err = db.Exec(sqlCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create the DB schema: %w", err)
	}

	instance := DBSQLite{db}
	return &instance, nil
}

func (db *DBSQLite) Close() error {
	return db.db.Close()
}

func (db *DBSQLite) UpsertNodeAddr(ctx context.Context, id NodeID, addr NodeAddr) error {
	var ip *string
	if addr.IP != nil {
		value := addr.IP.String()
		ip = &value
	}

	var ipV6 *string
	if addr.IPv6.IP != nil {
		value := addr.IPv6.IP.String()
		ipV6 = &value
	}

	var portDisc *int
	if (ip != nil) && (addr.PortDisc != 0) {
		value := int(addr.PortDisc)
		portDisc = &value
	}

	var ipV6PortDisc *int
	if (ipV6 != nil) && (addr.IPv6.PortDisc != 0) {
		value := int(addr.IPv6.PortDisc)
		ipV6PortDisc = &value
	}

	var portRLPx *int
	if (ip != nil) && (addr.PortRLPx != 0) {
		value := int(addr.PortRLPx)
		portRLPx = &value
	}

	var ipV6PortRLPx *int
	if (ipV6 != nil) && (addr.IPv6.PortRLPx != 0) {
		value := int(addr.IPv6.PortRLPx)
		ipV6PortRLPx = &value
	}

	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlUpsertNodeAddr,
		id,
		ip, portDisc, portRLPx,
		ipV6, ipV6PortDisc, ipV6PortRLPx,
		updated)
	if err != nil {
		return fmt.Errorf("failed to upsert a node address: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindNodeAddr(ctx context.Context, id NodeID) (*NodeAddr, error) {
	row := db.db.QueryRowContext(ctx, sqlFindNodeAddr, id)

	var ip sql.NullString
	var portDisc sql.NullInt32
	var portRLPx sql.NullInt32
	var ipV6 sql.NullString
	var ipV6PortDisc sql.NullInt32
	var ipV6PortRLPx sql.NullInt32

	err := row.Scan(
		&ip,
		&portDisc,
		&portRLPx,
		&ipV6,
		&ipV6PortDisc,
		&ipV6PortRLPx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("FindNodeAddr failed: %w", err)
	}

	var addr NodeAddr

	if ip.Valid {
		value := net.ParseIP(ip.String)
		if value == nil {
			return nil, errors.New("FindNodeAddr failed to parse IP")
		}
		addr.IP = value
	}
	if ipV6.Valid {
		value := net.ParseIP(ipV6.String)
		if value == nil {
			return nil, errors.New("FindNodeAddr failed to parse IPv6")
		}
		addr.IPv6.IP = value
	}
	if portDisc.Valid {
		value := uint16(portDisc.Int32)
		addr.PortDisc = value
	}
	if portRLPx.Valid {
		value := uint16(portRLPx.Int32)
		addr.PortRLPx = value
	}
	if ipV6PortDisc.Valid {
		value := uint16(ipV6PortDisc.Int32)
		addr.IPv6.PortDisc = value
	}
	if ipV6PortRLPx.Valid {
		value := uint16(ipV6PortRLPx.Int32)
		addr.IPv6.PortRLPx = value
	}

	return &addr, nil
}

func (db *DBSQLite) ResetPingError(ctx context.Context, id NodeID) error {
	_, err := db.db.ExecContext(ctx, sqlResetPingError, id)
	if err != nil {
		return fmt.Errorf("ResetPingError failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) UpdatePingError(ctx context.Context, id NodeID) error {
	_, err := db.db.ExecContext(ctx, sqlUpdatePingError, id)
	if err != nil {
		return fmt.Errorf("UpdatePingError failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) CountPingErrors(ctx context.Context, id NodeID) (*uint, error) {
	row := db.db.QueryRowContext(ctx, sqlCountPingErrors, id)
	var count uint
	if err := row.Scan(&count); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("CountPingErrors failed: %w", err)
	}
	return &count, nil
}

func (db *DBSQLite) UpdateClientID(ctx context.Context, id NodeID, clientID string) error {
	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlUpdateClientID, clientID, updated, id)
	if err != nil {
		return fmt.Errorf("UpdateClientID failed to update a node: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindClientID(ctx context.Context, id NodeID) (*string, error) {
	row := db.db.QueryRowContext(ctx, sqlFindClientID, id)
	var clientID sql.NullString
	err := row.Scan(&clientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("FindClientID failed: %w", err)
	}
	if clientID.Valid {
		return &clientID.String, nil
	}
	return nil, nil
}

func (db *DBSQLite) UpdateNetworkID(ctx context.Context, id NodeID, networkID uint) error {
	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlUpdateNetworkID, networkID, updated, id)
	if err != nil {
		return fmt.Errorf("UpdateNetworkID failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) UpdateEthVersion(ctx context.Context, id NodeID, ethVersion uint) error {
	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlUpdateEthVersion, ethVersion, updated, id)
	if err != nil {
		return fmt.Errorf("UpdateEthVersion failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) UpdateHandshakeTransientError(ctx context.Context, id NodeID, hasTransientErr bool) error {
	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlUpdateHandshakeTransientError, hasTransientErr, updated, id)
	if err != nil {
		return fmt.Errorf("UpdateHandshakeTransientError failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) InsertHandshakeError(ctx context.Context, id NodeID, handshakeErr string) error {
	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlInsertHandshakeError, id, handshakeErr, updated)
	if err != nil {
		return fmt.Errorf("InsertHandshakeError failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) DeleteHandshakeErrors(ctx context.Context, id NodeID) error {
	_, err := db.db.ExecContext(ctx, sqlDeleteHandshakeErrors, id)
	if err != nil {
		return fmt.Errorf("DeleteHandshakeErrors failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindHandshakeLastErrors(ctx context.Context, id NodeID, limit uint) ([]HandshakeError, error) {
	cursor, err := db.db.QueryContext(
		ctx,
		sqlFindHandshakeLastErrors,
		id,
		limit)
	if err != nil {
		return nil, fmt.Errorf("FindHandshakeLastErrors failed to query: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	var handshakeErrors []HandshakeError
	for cursor.Next() {
		var stringCode string
		var updatedTimestamp int64
		err := cursor.Scan(&stringCode, &updatedTimestamp)
		if err != nil {
			return nil, fmt.Errorf("FindHandshakeLastErrors failed to read data: %w", err)
		}

		handshakeError := HandshakeError{
			stringCode,
			time.Unix(updatedTimestamp, 0),
		}

		handshakeErrors = append(handshakeErrors, handshakeError)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("FindHandshakeLastErrors failed to iterate over rows: %w", err)
	}
	return handshakeErrors, nil
}

func (db *DBSQLite) UpdateHandshakeRetryTime(ctx context.Context, id NodeID, retryTime time.Time) error {
	_, err := db.db.ExecContext(ctx, sqlUpdateHandshakeRetryTime, retryTime.Unix(), id)
	if err != nil {
		return fmt.Errorf("UpdateHandshakeRetryTime failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindHandshakeRetryTime(ctx context.Context, id NodeID) (*time.Time, error) {
	row := db.db.QueryRowContext(ctx, sqlFindHandshakeRetryTime, id)

	var timestamp sql.NullInt64

	if err := row.Scan(&timestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("FindHandshakeRetryTime failed: %w", err)
	}

	// if never we tried to handshake then the time is NULL
	if !timestamp.Valid {
		return nil, nil
	}

	retryTime := time.Unix(timestamp.Int64, 0)
	return &retryTime, nil
}

func (db *DBSQLite) CountHandshakeCandidates(ctx context.Context) (uint, error) {
	retryTimeBefore := time.Now().Unix()
	row := db.db.QueryRowContext(ctx, sqlCountHandshakeCandidates, retryTimeBefore)
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountHandshakeCandidates failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) FindHandshakeCandidates(
	ctx context.Context,
	limit uint,
) ([]NodeID, error) {
	retryTimeBefore := time.Now().Unix()
	cursor, err := db.db.QueryContext(
		ctx,
		sqlFindHandshakeCandidates,
		retryTimeBefore,
		limit)
	if err != nil {
		return nil, fmt.Errorf("FindHandshakeCandidates failed to query candidates: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	var nodes []NodeID
	for cursor.Next() {
		var id string
		err := cursor.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("FindHandshakeCandidates failed to read candidate data: %w", err)
		}

		nodes = append(nodes, NodeID(id))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("FindHandshakeCandidates failed to iterate over candidates: %w", err)
	}
	return nodes, nil
}

func (db *DBSQLite) MarkTakenHandshakeCandidates(ctx context.Context, ids []NodeID) error {
	if len(ids) == 0 {
		return nil
	}

	delayedRetryTime := time.Now().Add(time.Hour).Unix()

	idsPlaceholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	query := strings.Replace(sqlMarkTakenHandshakeCandidates, "123", idsPlaceholders, 1)
	args := append([]interface{}{delayedRetryTime}, stringsToAny(ids)...)

	_, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to mark taken handshake candidates: %w", err)
	}
	return nil
}

func (db *DBSQLite) TakeHandshakeCandidates(
	ctx context.Context,
	limit uint,
) ([]NodeID, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("TakeHandshakeCandidates failed to start transaction: %w", err)
	}

	ids, err := db.FindHandshakeCandidates(
		ctx,
		limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = db.MarkTakenHandshakeCandidates(ctx, ids)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("TakeHandshakeCandidates failed to commit transaction: %w", err)
	}
	return ids, nil
}

func (db *DBSQLite) UpdateForkCompatibility(ctx context.Context, id NodeID, isCompatFork bool) error {
	updated := time.Now().Unix()

	_, err := db.db.ExecContext(ctx, sqlUpdateForkCompatibility, isCompatFork, updated, id)
	if err != nil {
		return fmt.Errorf("UpdateForkCompatibility failed to update a node: %w", err)
	}
	return nil
}

func (db *DBSQLite) UpdateNeighborBucketKeys(ctx context.Context, id NodeID, keys []string) error {
	keysStr := strings.Join(keys, ",")

	_, err := db.db.ExecContext(ctx, sqlUpdateNeighborBucketKeys, keysStr, id)
	if err != nil {
		return fmt.Errorf("UpdateNeighborBucketKeys failed to update a node: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindNeighborBucketKeys(ctx context.Context, id NodeID) ([]string, error) {
	row := db.db.QueryRowContext(ctx, sqlFindNeighborBucketKeys, id)

	var keysStr sql.NullString
	if err := row.Scan(&keysStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("FindNeighborBucketKeys failed: %w", err)
	}

	if !keysStr.Valid {
		return nil, nil
	}
	return utils.SplitAndTrim(keysStr.String), nil
}

func (db *DBSQLite) UpdateSentryCandidatesLastEventTime(ctx context.Context, value time.Time) error {
	_, err := db.db.ExecContext(ctx, sqlUpdateSentryCandidatesLastEventTime, value.Unix())
	if err != nil {
		return fmt.Errorf("UpdateSentryCandidatesLastEventTime failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindSentryCandidatesLastEventTime(ctx context.Context) (*time.Time, error) {
	row := db.db.QueryRowContext(ctx, sqlFindSentryCandidatesLastEventTime)

	var timestamp sql.NullInt64
	if err := row.Scan(&timestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("FindSentryCandidatesLastEventTime failed: %w", err)
	}

	value := time.Unix(timestamp.Int64, 0)
	return &value, nil
}

func (db *DBSQLite) UpdateCrawlRetryTime(ctx context.Context, id NodeID, retryTime time.Time) error {
	_, err := db.db.ExecContext(ctx, sqlUpdateCrawlRetryTime, retryTime.Unix(), id)
	if err != nil {
		return fmt.Errorf("UpdateCrawlRetryTime failed: %w", err)
	}
	return nil
}

func (db *DBSQLite) CountCandidates(ctx context.Context) (uint, error) {
	retryTimeBefore := time.Now().Unix()
	row := db.db.QueryRowContext(ctx, sqlCountCandidates, retryTimeBefore)
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountCandidates failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) FindCandidates(
	ctx context.Context,
	limit uint,
) ([]NodeID, error) {
	retryTimeBefore := time.Now().Unix()
	cursor, err := db.db.QueryContext(
		ctx,
		sqlFindCandidates,
		retryTimeBefore,
		limit)
	if err != nil {
		return nil, fmt.Errorf("FindCandidates failed to query candidates: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	var nodes []NodeID
	for cursor.Next() {
		var id string
		err := cursor.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("FindCandidates failed to read candidate data: %w", err)
		}

		nodes = append(nodes, NodeID(id))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("FindCandidates failed to iterate over candidates: %w", err)
	}
	return nodes, nil
}

func (db *DBSQLite) MarkTakenNodes(ctx context.Context, ids []NodeID) error {
	if len(ids) == 0 {
		return nil
	}

	delayedRetryTime := time.Now().Add(time.Hour).Unix()

	idsPlaceholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	query := strings.Replace(sqlMarkTakenNodes, "123", idsPlaceholders, 1)
	args := append([]interface{}{delayedRetryTime}, stringsToAny(ids)...)

	_, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to mark taken nodes: %w", err)
	}
	return nil
}

func (db *DBSQLite) TakeCandidates(
	ctx context.Context,
	limit uint,
) ([]NodeID, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("TakeCandidates failed to start transaction: %w", err)
	}

	ids, err := db.FindCandidates(
		ctx,
		limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = db.MarkTakenNodes(ctx, ids)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("TakeCandidates failed to commit transaction: %w", err)
	}
	return ids, nil
}

func (db *DBSQLite) IsConflictError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "SQLITE_BUSY")
}

func (db *DBSQLite) CountNodes(ctx context.Context, maxPingTries uint, networkID uint) (uint, error) {
	row := db.db.QueryRowContext(ctx, sqlCountNodes, maxPingTries, networkID)
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountNodes failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) CountIPs(ctx context.Context, maxPingTries uint, networkID uint) (uint, error) {
	row := db.db.QueryRowContext(ctx, sqlCountIPs, maxPingTries, networkID)
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountIPs failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) CountClients(ctx context.Context, clientIDPrefix string, maxPingTries uint, networkID uint) (uint, error) {
	row := db.db.QueryRowContext(ctx, sqlCountClients, maxPingTries, networkID, clientIDPrefix+"%")
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountClients failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) CountClientsWithNetworkID(ctx context.Context, clientIDPrefix string, maxPingTries uint) (uint, error) {
	row := db.db.QueryRowContext(ctx, sqlCountClientsWithNetworkID, maxPingTries, clientIDPrefix+"%")
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountClientsWithNetworkID failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) CountClientsWithHandshakeTransientError(ctx context.Context, clientIDPrefix string, maxPingTries uint) (uint, error) {
	row := db.db.QueryRowContext(ctx, sqlCountClientsWithHandshakeTransientError, maxPingTries, clientIDPrefix+"%")
	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("CountClientsWithHandshakeTransientError failed: %w", err)
	}
	return count, nil
}

func (db *DBSQLite) EnumerateClientIDs(
	ctx context.Context,
	maxPingTries uint,
	networkID uint,
	enumFunc func(clientID *string),
) error {
	cursor, err := db.db.QueryContext(ctx, sqlEnumerateClientIDs, maxPingTries, networkID)
	if err != nil {
		return fmt.Errorf("EnumerateClientIDs failed to query: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	for cursor.Next() {
		var clientID sql.NullString
		err := cursor.Scan(&clientID)
		if err != nil {
			return fmt.Errorf("EnumerateClientIDs failed to read data: %w", err)
		}
		if clientID.Valid {
			enumFunc(&clientID.String)
		} else {
			enumFunc(nil)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("EnumerateClientIDs failed to iterate: %w", err)
	}
	return nil
}

func stringsToAny(strValues []NodeID) []interface{} {
	values := make([]interface{}, 0, len(strValues))
	for _, value := range strValues {
		values = append(values, value)
	}
	return values
}
