package connpostgres

import (
	"context"
	"log/slog"
	"regexp"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *PostgresConnector) CreateTableInSchemaDDL(ctx context.Context, schema string, table string, columns []*protos.ColumnsItem) (string, error) {
	createTableQuery := "CREATE TABLE " + schema + "." + table + " ("
	for i, column := range columns {
		createTableQuery += column.Name + " " + column.Type
		if column.IsKey {
			createTableQuery += " PRIMARY KEY"
		}
		if i < len(columns)-1 {
			createTableQuery += ", "
		}
	}
	createTableQuery += ");"

	return createTableQuery, nil
}

func (c *PostgresConnector) GetIndexesInSchema(ctx context.Context, schema string) (map[string]string, error) {
	query := `SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1;`
	rows, err := c.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	indexes := make(map[string]string)

	for rows.Next() {
		var indexName, indexDef string
		if err := rows.Scan(&indexName, &indexDef); err != nil {
			return nil, err
		}

		// TODO: better alternative is to create an ast and modifying that
		if !strings.Contains(strings.ToUpper(indexDef), "IF NOT EXISTS") {
			re := regexp.MustCompile(`(?i)^(CREATE\s+(?:UNIQUE\s+)?INDEX)\s+`)
			result := re.ReplaceAllString(indexDef, "${1} IF NOT EXISTS ")
			indexDef = result
		}
		indexes[indexName] = indexDef + ";"
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return indexes, nil
}

func (c *PostgresConnector) GetViewsInSchema(ctx context.Context, schema string) (map[string]string, error) {
	query := `SELECT viewname, definition FROM pg_views WHERE schemaname = $1;`
	rows, err := c.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	views := make(map[string]string)

	for rows.Next() {
		var viewName, viewDef string
		if err := rows.Scan(&viewName, &viewDef); err != nil {
			return nil, err
		}
		views[viewName] = "CREATE OR REPLACE VIEW " + viewName + " AS " + viewDef
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return views, nil
}

func (c *PostgresConnector) GetFunctionsInSchema(ctx context.Context, schema string) (map[string]string, error) {
	query := `SELECT p.proname,pg_get_functiondef(p.oid) FROM pg_proc p
				JOIN pg_namespace n ON n.oid = p.pronamespace
				WHERE n.nspname = $1;`
	rows, err := c.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	functions := make(map[string]string)

	for rows.Next() {
		var funcName, funcDef string
		if err := rows.Scan(&funcName, &funcDef); err != nil {
			return nil, err
		}
		functions[funcName] = funcDef + ";"
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return functions, nil
}

func (c *PostgresConnector) GetTriggersInSchema(ctx context.Context, schema string) (map[string]string, error) {
	query := `SELECT 
				tgname AS trigger,
				pg_get_triggerdef(oid, true) AS definition
			FROM pg_trigger
			WHERE NOT tgisinternal
			AND tgrelid IN (
				SELECT oid FROM pg_class WHERE relnamespace = 
					(SELECT oid FROM pg_namespace WHERE nspname = $1));`

	rows, err := c.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	triggers := make(map[string]string)

	for rows.Next() {
		var triggerName, triggerDef string
		if err := rows.Scan(&triggerName, &triggerDef); err != nil {
			return nil, err
		}
		triggers[triggerName] = triggerDef + ";"
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return triggers, nil
}

func (c *PostgresConnector) ExecuteDDL(ctx context.Context, ddl string) error {
	c.logger.Info("executing ddl", slog.String("ddl", ddl))
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, ddl)
	if err != nil {
		c.logger.Error("error executing ddl statement, rolling back", slog.Any("error", err))
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil {
			c.logger.Error("error rolling back ddl statement", slog.Any("error", rollbackErr))
		}
		return err
	}

	return tx.Commit(ctx)

}
