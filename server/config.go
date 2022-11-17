package server

import (
	md52 "crypto/md5"
	"encoding/hex"
	"io"
	"os"

	"github.com/BurntSushi/toml"

	"github.com/flower-corp/rosedb/logger"
)

type LoadCmd int

const (
	Nothing LoadCmd = 0
	Load            = 1 //The config file should be loaded as init loading. feature off -> on
	Reload          = 2 //should be loaded again. feature on -> off -> on, or on -> on
	Unload          = 3 //The config file disapear, so just turn off your feature!. feature on->off
)

type Config struct {
	DBFilePath   string `toml:"db_file_path"`
	Host         string `toml:"host_url"`
	Port         string `toml:"port"`
	DataBasesNum int8   `toml:"database_num"`

	IndexMode         int8   `toml:"index_mode"`
	IoType            int8   `toml:"io_type"`
	Sync              bool   `toml:"sync_file"`
	LogFileGCInterval int64  `toml:"logfile_gc_interval"`
	LogFileGCRatio    string `toml:"logfile_gc_ratio"`
}

type RoseDBCfg struct {
	RoseConfig Config
}

type LoadConfig struct {
	AppConfigPath string
	MD5           string
	LoadF         func(string, LoadCmd)
}

var DBConfig Config

func NewConfigToml(cfgName string, v interface{}) *LoadConfig {
	return NewConfig(cfgName, false, func(s string, cmd LoadCmd) {
		if cmd == Load {
			if _, err := toml.DecodeFile(s, v); err != nil {
				logger.Errorf("load roseDB config err %v", err)
			} else {
				logger.Infof("roseDB config load success %s", s)
			}
		} else {
			logger.Infof("roseDB does not support reload or unload")
		}
	})
}

func NewConfig(cfgName string, reload bool, loadFunc func(string, LoadCmd)) *LoadConfig {
	var cfg LoadConfig
	cfg.AppConfigPath = "./conf/" + cfgName
	file, err := os.Open(cfg.AppConfigPath)
	if err != nil {
		logger.Errorf("config file not exist %s, %v", cfg.AppConfigPath, err)
		return nil
	}
	md5 := md52.New()
	io.Copy(md5, file)
	cfg.MD5 = hex.EncodeToString(md5.Sum(nil))
	if loadFunc != nil {
		cfg.LoadF = loadFunc
		loadFunc(cfg.AppConfigPath, Load)
	}
	return &cfg
}
