package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type Project struct {
	Id     int    `json:"id"`
	Sender string `json:"sender" binding:"required"`
	Method string `json:"method" binding:"required"`
	Onb    int    `json:"onb" binding:"required"`
	PrjNum string `json:"prj_num"`
	Status string `json:"transfer_status" binding:"required"`
}

type NotificationPayload struct {
	Timestamp time.Time `json:"timestemp"`
	Action    string    `json:"action"`
	Schema    string    `json:"schema"`
	Identity  string    `json:"identity"`
	New       Project   `json:"new"`
	Old       Project   `json:"old"`
}

var pool *pgxpool.Pool
var riverClient *river.Client[pgx.Tx]

const DATABASE_DSN string = "application_name=Tests user=postgres password=postgres host=postgres dbname=postgres"

func createProjectsTable(tx pgx.Tx, ctx context.Context) error {
	_, err := tx.Exec(ctx, `CREATE SEQUENCE IF NOT EXISTS "Projects_id_seq";`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS "Projects" (
			"id" integer DEFAULT nextval('"Projects_id_seq"') NOT NULL,
			"sender" text DEFAULT 'IS-SYSTEM' NOT NULL,
			"method" text,
			"onb" integer NOT NULL,
			"prj_num" text,
			"transfer_status" character(1) DEFAULT 'U' NOT NULL,
			CONSTRAINT "Projects_pkey" PRIMARY KEY ("id")
		) WITH (oids = false);`)

	return err
}

func createProjectsNotification(tx pgx.Tx, ctx context.Context) error {
	_, err := tx.Exec(ctx, `
		CREATE OR REPLACE FUNCTION prj_notify() RETURNS trigger AS $trigger$
		DECLARE
		rec RECORD;
		dat RECORD;
		payload TEXT;
		BEGIN
		
		-- Set record row depending on operation
		CASE TG_OP
		WHEN 'UPDATE' THEN
			rec := NEW;
			dat := OLD;
		WHEN 'INSERT' THEN
			rec := NEW;
		WHEN 'DELETE' THEN
			rec := OLD;
		ELSE
			RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;
		END CASE;
		
		-- Build the payload
		payload := json_build_object(
			'timestamp', CURRENT_TIMESTAMP,
			'action', LOWER(TG_OP),
			'schema', TG_TABLE_SCHEMA,
			'identity', TG_TABLE_NAME,
			'new', row_to_json(rec),
			'old', row_to_json(dat)
		);
		
		-- Notify the channel
		PERFORM pg_notify('projects', payload);
		
		RETURN rec;
		END;
		$trigger$ LANGUAGE plpgsql;`)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating function: %v\n", err)
		return err
	}

	_, err = tx.Exec(ctx, `
		CREATE OR REPLACE TRIGGER projects_notify
		AFTER INSERT OR UPDATE OR DELETE ON "Projects" FOR EACH ROW
		EXECUTE PROCEDURE prj_notify();`)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating trigger: %v\n", err)
		return err
	}

	return nil
}

func setupDB(pool *pgxpool.Pool) error {
	var err error

	// Start a transaction and handle the closing
	tx, err := pool.Begin(context.TODO())
	if err != nil {
		return err
	}

	transactionCtx, transactionCtxCancel := context.WithTimeout(context.Background(), time.Second*15)
	defer transactionCtxCancel()

	err = createProjectsTable(tx, transactionCtx)
	if err != nil {
		tx.Rollback(context.TODO())
		return err
	}
	err = createProjectsNotification(tx, transactionCtx)
	if err != nil {
		tx.Rollback(context.TODO())
		return err
	}

	return tx.Commit(context.TODO())
}

func getData(c *gin.Context) {
	rows, err := pool.Query(context.TODO(), `select id, sender, method, onb, prj_num, transfer_status from "Projects"`)
	if err != nil {
		c.String(http.StatusInternalServerError, "%s", err)
		return
	}

	var data []Project
	for rows.Next() {
		var tmp Project
		err = rows.Scan(&tmp.Id, &tmp.Sender, &tmp.Method, &tmp.Onb, &tmp.PrjNum, &tmp.Status)
		if err != nil {
			break
		}
		data = append(data, tmp)
	}
	if err != nil {
		c.String(http.StatusInternalServerError, "%s", err)
		return
	}
	c.IndentedJSON(http.StatusOK, data)
}

func postData(c *gin.Context) {
	var data QueueTestArgs
	err := c.BindJSON(&data)

	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
	}
	fmt.Println("Queuing job", data.Onb)

	transactionTimeoutCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*250)
	defer cancel()

	var tx pgx.Tx
	tx, err = pool.Begin(transactionTimeoutCtx)

	if err != nil { // Check if transaction started
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	_, err = riverClient.InsertTx(transactionTimeoutCtx, tx, data, nil)
	if err != nil { // Check if item was queued
		err = tx.Rollback(transactionTimeoutCtx)
		c.AbortWithError(http.StatusInternalServerError, err)
	} else {
		// On successful queue, commit...
		err = tx.Commit(transactionTimeoutCtx)
		if err != nil { // ...and check if the commit succeeded
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// select {
	// case <-transactionTimeoutCtx.Done():
	// 	c.AbortWithStatus(http.StatusRequestTimeout)
	// default:
	// 	// SUCCESS
	// 	c.Status(http.StatusOK)
	// }
	// SUCCESS
	c.Status(http.StatusOK)
}

func handleProjectsNotification(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
	fmt.Printf("%s - %d - %s", notification.Channel, notification.PID, notification.Payload)

	var payload NotificationPayload
	err := json.Unmarshal([]byte(notification.Payload), &payload)
	if err != nil {
		return err
	}

	// Do sth. with the payload.

	return nil
}

type QueueTestArgs Project

func (QueueTestArgs) Kind() string { return "queuetest" }

type TestWorker struct {
	river.WorkerDefaults[QueueTestArgs]
}

func (w *TestWorker) Work(ctx context.Context, job *river.Job[QueueTestArgs]) error {
	fmt.Println("Started working on", job.ID)
	// if err != nil {
	// 	c.String(http.StatusBadRequest, err.Error())
	// }

	workContext, workContextCancel := context.WithTimeout(ctx, time.Second*30)
	defer workContextCancel()

	tx, err := pool.Begin(workContext)
	if err != nil {
		return err
	}

	data := job.Args
	_, insertErr := tx.Exec(workContext, `Insert into "Projects" (sender, method, onb, prj_num, transfer_status) VALUES ($1, $2, $3, $4, $5)`,
		data.Sender, data.Method, data.Onb, data.PrjNum, data.Status)
	if insertErr != nil {
		rollbackErr := tx.Rollback(workContext)
		if rollbackErr != nil {
			return fmt.Errorf("error rolling back transaction: %w because of insert error: %w", rollbackErr, insertErr)
		}
		return insertErr
	}
	return tx.Commit(workContext)
}

func main() {
	var err error

	// DB Connection

	pool, err = pgxpool.New(context.TODO(), DATABASE_DSN)
	if err != nil {
		fmt.Printf("Unable to create connection pool: %s\n", err)
	}
	defer pool.Close()

	err = setupDB(pool)
	if err != nil {
		fmt.Printf("Unable to set up database: %s", err)
		os.Exit(1)
	}

	// Queue

	workers := river.NewWorkers()
	river.AddWorkerSafely(workers, &TestWorker{})

	riverClient, _ = river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	})

	// Listener

	listener := pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			// return pool.Acquire() // notifications with pool-connections are unreliable
			return pgx.Connect(context.TODO(), DATABASE_DSN)
		},
		LogError: func(ctx context.Context, err error) {
			fmt.Println(err)
		},
	}

	listener.Handle("projects", pgxlisten.HandlerFunc(handleProjectsNotification))

	// Server

	router := gin.Default()
	server := &http.Server{Addr: ":8080", Handler: router}

	serverShutdownChan := make(chan bool, 1)

	router.GET("/data", getData)
	router.POST("/data", postData)

	// Route to gracefully shutdown
	router.GET("/stop", func(ctx *gin.Context) {
		fmt.Println("/stop called")

		fmt.Println("sending signal to stop server")
		serverShutdownChan <- true

		ctx.Status(200)
	})

	// Handle os signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Printf("Received signal %s\n", sig.String())
		serverShutdownChan <- true
	}()

	// This go-routine waits for the shutdown signal (from /stop) and stops the http server, which then unblocks and ends the program gracefully.
	go func() {
		<-serverShutdownChan
		fmt.Println("Starting shutdown routine")

		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		err := server.Shutdown(timeoutCtx)
		fmt.Println("Server stopped", err)
	}()

	// Start the queue
	go func() {
		err := riverClient.Start(context.Background())
		if err != nil {
			panic(fmt.Sprintf("Error starting queue: %s", err))
		}
	}()

	// Start the listener
	listenerCtx, listenerCtxCancel := context.WithCancel(context.Background())
	defer listenerCtxCancel()
	go listener.Listen(listenerCtx)

	// Start the webserver
	err = server.ListenAndServe() // The server blocks until it's shut down
	if err != nil {
		fmt.Println(err)
	}

	// Shutdown routine

	fmt.Println("Stopping queue")
	riverTimeoutCtx, riverTimeoutCtxCancel := context.WithTimeout(context.Background(), time.Second*10)
	defer riverTimeoutCtxCancel()

	err = riverClient.Stop(riverTimeoutCtx)
	fmt.Printf("Queue stopped: %v\n", err)

	fmt.Println("Stopping listener")
	listenerCtxCancel()
	fmt.Printf("Listener stopped: %v\n", listenerCtx.Err())
}
