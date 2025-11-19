package connpostgres

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *PostgresConnector) CreateTableInSchema(ctx context.Context, schema string, table string, columns []*protos.ColumnsItem) error {
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

	c.logger.Info("------- ", createTableQuery)

	// _, err := c.conn.Exec(ctx, createTableQuery)
	// if err != nil {
	// 	c.logger.Error("error creating table from schema", slog.Any("error", err))
	// 	return err
	// }
	return nil
}

func (c *PostgresConnector) CreateIndexInSchema(ctx context.Context, schema string, index_ddl string) error {
	c.logger.Info("------- ", index_ddl)
	// _, err := c.conn.Exec(ctx, index_ddl)
	// if err != nil {
	// 	c.logger.Error("error creating index from schema", slog.Any("error", err))
	// 	return err
	// }
	return nil
}

func (c *PostgresConnector) CreateViewInSchema(ctx context.Context, schema string, view_ddl string) error {
	c.logger.Info("------- ", view_ddl)
	// _, err := c.conn.Exec(ctx, view_ddl)
	// if err != nil {
	// 	c.logger.Error("error creating view from schema", slog.Any("error", err))
	// 	return err
	// }
	return nil
}

func (c *PostgresConnector) CreateFunctionInSchema(ctx context.Context, schema string, function_ddl string) error {
	c.logger.Info("------- ", function_ddl)
	// _, err := c.conn.Exec(ctx, function_ddl)
	// if err != nil {
	// 	c.logger.Error("error creating function from schema", slog.Any("error", err))
	// 	return err
	// }
	return nil
}

func (c *PostgresConnector) CreateTriggerInSchema(ctx context.Context, schema string, trigger_ddl string) error {
	c.logger.Info("------- ", trigger_ddl)
	// _, err := c.conn.Exec(ctx, trigger_ddl)
	// if err != nil {
	// 	c.logger.Error("error creating trigger from schema", slog.Any("error", err))
	// 	return err
	// }
	return nil
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
		indexes[indexName] = indexDef
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
		views[viewName] = viewDef
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
	rows, err := c.conn.Query(ctx, query)
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
		functions[funcName] = funcDef
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

	rows, err := c.conn.Query(ctx, query)
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
		triggers[triggerName] = triggerDef
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return triggers, nil
}
