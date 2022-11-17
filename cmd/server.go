package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/tidwall/redcon"

	"github.com/flower-corp/rosedb"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/server"
)

func init() {
	// print banner
	path, _ := filepath.Abs("resource/banner.txt")
	banner, _ := ioutil.ReadFile(path)
	fmt.Println(string(banner))
}

type Server struct {
	dbs    map[int]*rosedb.RoseDB
	ser    *redcon.Server
	signal chan os.Signal
	opts   ServerOptions
	mu     *sync.RWMutex
}

type ServerOptions struct {
	dbPath    string
	host      string
	port      string
	databases uint
}

func main() {
	var RoseCfg server.RoseDBCfg
	server.NewConfigToml("rose.toml", &RoseCfg)

	serverOpts := &ServerOptions{
		dbPath:    RoseCfg.RoseConfig.DBFilePath,
		host:      RoseCfg.RoseConfig.Host,
		port:      RoseCfg.RoseConfig.Port,
		databases: uint(RoseCfg.RoseConfig.DataBasesNum),
	}

	// open a default database
	path := filepath.Join(serverOpts.dbPath, fmt.Sprintf(server.DBName, server.DefaultDB))
	opts := rosedb.DefaultOptions(RoseCfg.RoseConfig, path)
	now := time.Now()
	db, err := rosedb.Open(opts)
	if err != nil {
		logger.Errorf("open rosedb err, fail to start server. %v", err)
		return
	}
	logger.Infof("open db from [%s] successfully, time cost: %v", serverOpts.dbPath, time.Since(now))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	dbs := make(map[int]*rosedb.RoseDB)
	dbs[0] = db

	// init and start server
	svr := &Server{dbs: dbs, signal: sig, opts: *serverOpts, mu: new(sync.RWMutex)}
	addr := svr.opts.host + ":" + svr.opts.port
	redServer := redcon.NewServerNetwork("tcp", addr, execClientCommand, svr.redconAccept,
		func(conn redcon.Conn, err error) {
		},
	)
	svr.ser = redServer
	go svr.listen()
	<-svr.signal
	svr.stop()
}

func (svr *Server) listen() {
	logger.Infof("rosedb server is running, ready to accept connections")
	if err := svr.ser.ListenAndServe(); err != nil {
		logger.Fatalf("listen and serve err, fail to start. %v", err)
		return
	}
}

func (svr *Server) stop() {
	for _, db := range svr.dbs {
		if err := db.Close(); err != nil {
			logger.Errorf("close db err: %v", err)
		}
	}
	if err := svr.ser.Close(); err != nil {
		logger.Errorf("close server err: %v", err)
	}
	logger.Infof("rosedb is ready to exit, bye bye...")
}

func (svr *Server) redconAccept(conn redcon.Conn) bool {
	cli := new(Client)
	cli.svr = svr
	svr.mu.RLock()
	cli.db = svr.dbs[0]
	svr.mu.RUnlock()
	conn.SetContext(cli)
	return true
}
