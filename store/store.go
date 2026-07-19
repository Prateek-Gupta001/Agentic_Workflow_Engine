// store/store.go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type Store struct{ db *sql.DB }

func NewStore() (*Store, error) {
	dbPassword := os.Getenv("DB_PASSWORD")
	connStr := fmt.Sprintf("host=127.0.0.1 port=5432 user=postgres dbname=postgres password=%s sslmode=disable", dbPassword)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("pinging db: %w", err)
	}
	return &Store{db: db}, nil
}

// InitSchema uses IF NOT EXISTS (a deliberate addition to your DDL) so the
// binary can call this on every startup without erroring on a live DB.
func (s *Store) InitSchema(ctx context.Context) error {
	const schema = `
	CREATE EXTENSION IF NOT EXISTS pgcrypto;

	CREATE TABLE IF NOT EXISTS workflow_runs (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		workflow_id TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'running',
		input JSONB NOT NULL,
		output JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	);

	CREATE TABLE IF NOT EXISTS node_states (
		run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
		node_id TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		input JSONB,
		output JSONB,
		error TEXT,
		attempt_count INT NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (run_id, node_id)
	);
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// What are the different methods that we really want for this ...
// We need to create a new run whenever we get a new request.
func (s *Store) CreateRun(ctx context.Context, runId string, workflowID string, input map[string]any, nodeIDs []string) (string, error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("marshaling input: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var runID string
	err = tx.QueryRowContext(ctx,
		`INSERT INTO workflow_runs (id, workflow_id, input) VALUES ($1, $2, $3) RETURNING id`,
		runId, workflowID, inputJSON,
	).Scan(&runID)
	if err != nil {
		return "", fmt.Errorf("inserting run: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO node_states (run_id, node_id, status) VALUES ($1, $2, 'pending')`)
	if err != nil {
		return "", err
	}
	defer stmt.Close()

	for _, nodeID := range nodeIDs {
		if _, err := stmt.ExecContext(ctx, runID, nodeID); err != nil {
			return "", fmt.Errorf("seeding node %s: %w", nodeID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}
	return runID, nil
}

// This method stores the progress of a node.
func (s *Store) SaveNodeProgress(ctx context.Context, runId string, nodeId string, input map[string]any, output map[string]any) error {
	var inputJson, outputJson []byte
	var err error
	if input != nil {
		if inputJson, err = json.Marshal(input); err != nil {
			return fmt.Errorf("marshaling input for node %s: %w", nodeId, err)
		}
	}

	if output != nil {
		if outputJson, err = json.Marshal(output); err != nil {
			return fmt.Errorf("marshaling output for node %s: %w", nodeId, err)
		}
	}
	query := `
		UPDATE node_states 
		SET 
			status = 'completed',
			input = COALESCE($1, input), 
			output = COALESCE($2, output),
			updated_at = now()
		WHERE run_id = $3 AND node_id = $4
	`
	_, err = s.db.ExecContext(ctx, query, inputJson, outputJson, runId, nodeId)
	if err != nil {
		//TODO: Add a log here.
		return fmt.Errorf("Got this error while saving node progress: %w", err)
	}
	return nil
}

type NodeState struct {
	NodeID       string
	Status       string
	Input        map[string]any
	Output       map[string]any
	Error        string
	AttemptCount int
}

// This function gets the progress/current snapshot of the map that the DAG is working with.
// It rebuilds the entire map by rebuilding from the inputs from the other nodes.
// This is called before the execution of a node to pass this as the input to the node.
func (s *Store) GetNodeStates(ctx context.Context, runID string) (map[string]NodeState, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, status, input, output, error, attempt_count
		 FROM node_states WHERE run_id = $1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := make(map[string]NodeState)
	for rows.Next() {
		var (
			nodeID                  string
			status                  string
			inputBytes, outputBytes []byte
			errStr                  sql.NullString
			attemptCount            int
		)
		if err := rows.Scan(&nodeID, &status, &inputBytes, &outputBytes, &errStr, &attemptCount); err != nil {
			return nil, err
		}
		st := NodeState{NodeID: nodeID, Status: status, AttemptCount: attemptCount, Error: errStr.String}
		if len(inputBytes) > 0 {
			if err := json.Unmarshal(inputBytes, &st.Input); err != nil {
				return nil, fmt.Errorf("unmarshaling input for %s: %w", nodeID, err)
			}
		}
		if len(outputBytes) > 0 {
			if err := json.Unmarshal(outputBytes, &st.Output); err != nil {
				return nil, fmt.Errorf("unmarshaling output for %s: %w", nodeID, err)
			}
		}
		states[nodeID] = st
	}
	return states, rows.Err()
}

//Okay so now we have the methods for
//1. Saving the node progress (progress of an individual node)
//2. Getting the progress of the nodes (the DAG essentially)
//3. We do need to have a function for marking a node as running and incrementing it's attempt count.
//4. We also need one method for saving a failed node state.

// This function marks a node as running and increments it's attempt counter.
func (s *Store) MarkAsRunning(ctx context.Context, runId string, nodeId string, input map[string]any) error {
	query := `UPDATE node_states 
	SET 
	status = 'running',
	input = COALESCE($1, input), 
	attempt_count = attempt_count + 1,
	updated_at = now()
	WHERE
	run_id = $2 AND node_id = $3
	`
	var inputJson []byte
	var err error
	if input != nil {
		if inputJson, err = json.Marshal(input); err != nil {
			return fmt.Errorf("marshaling input for node %s: %w", nodeId, err)
		}
	}
	_, err = s.db.ExecContext(ctx, query, inputJson, runId, nodeId)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) MarkAsFailed(ctx context.Context, runId string, nodeId string, input map[string]any, Error string) error {
	var inputJson []byte
	var err error
	if input != nil {
		if inputJson, err = json.Marshal(input); err != nil {
			return fmt.Errorf("marshaling input for node %s: %w", nodeId, err)
		}
	}
	query := `UPDATE node_states 
	SET 
	status = 'failed',
	input = COALESCE($1, input),
	error = $2,
	updated_at = now()
	WHERE
	run_id = $3 AND node_id = $4
	`
	_, err = s.db.ExecContext(ctx, query, inputJson, Error, runId, nodeId)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) MarkAsSkipped(ctx context.Context, runId string, nodeIds []string) error {
	query := `UPDATE node_states 
	SET 
	status = 'skipped',
	updated_at = now()
	WHERE 
	run_id = $1 AND node_id = $2
	`
	var errs []error
	for _, node := range nodeIds {
		if _, err := s.db.ExecContext(ctx, query, runId, node); err != nil {
			errs = append(errs, fmt.Errorf("skipping %s: %w", node, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Store) MarkAwaitingApproval(ctx context.Context, runId, nodeId string, input map[string]any) error {
	var inputJson []byte
	var err error
	if input != nil {
		if inputJson, err = json.Marshal(input); err != nil {
			return fmt.Errorf("marshaling input for node %s: %w", nodeId, err)
		}
	}
	const query = `UPDATE node_states SET status = 'awaiting_approval', input = COALESCE($1, input), updated_at = now() WHERE run_id = $2 AND node_id = $3`
	_, err = s.db.ExecContext(ctx, query, inputJson, runId, nodeId)
	return err
}

func (s *Store) ResetToPending(ctx context.Context, runId, nodeId string) (bool, error) {
	const query = `UPDATE node_states SET status = 'pending', error = NULL, updated_at = now() WHERE run_id = $1 AND node_id = $2 AND status = 'failed'`
	res, err := s.db.ExecContext(ctx, query, runId, nodeId)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}
