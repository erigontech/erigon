package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/tracker"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const DefaultInterval = 60 //in seconds
const SoftLimit = 5 //in seconds
const DisconnectInterval = time.Minute //in seconds
var trackerID = "tg snapshot tracker"

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
}

func Execute() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
}

var rootCmd = &cobra.Command{
	Use:   "start",
	Short: "start tracker",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	Args:       cobra.ExactArgs(1),
	ArgAliases: []string{"snapshots dir"},
	RunE: func(cmd *cobra.Command, args []string) error {
		db:=ethdb.MustOpen(args[0])
		m := http.NewServeMux()
		m.Handle("/announce", &Tracker{db: db})
		m.HandleFunc("/scrape", func(writer http.ResponseWriter, request *http.Request) {
			log.Warn("scrape","url", request.RequestURI)
			ih:=request.URL.Query().Get("info_hash")
			if len(ih)!=20 {
				log.Error("wronng infohash","ih", ih, "l", len(ih))
				WriteResp(writer, ErrResponse{FailureReason: "incorrect infohash"}, false)
				return
			}
			resp :=ScrapeResponse{Files: map[string]*ScrapeData{
				ih:{},
			}}

			err := db.Walk(dbutils.SnapshotInfoBucket, append([]byte(ih), make([]byte, 20)...), 20*8, func(k, v []byte) (bool, error) {
				a:=AnnounceReqWithTime{}
				err := json.Unmarshal(v, &a)
				if err!=nil {
					log.Error("Fail to unmarshall", "k", common.Bytes2Hex(k), "err", err)
					//skip failed
					return true, nil
				}
				if time.Now().Sub(a.UpdatedAt) > 24*time.Hour {
					log.Debug("Skipped", "k", common.Bytes2Hex(k), "last updated", a.UpdatedAt)
					return true, nil
				}
				if a.Left==0 {
					resp.Files[ih].Downloaded++
					resp.Files[ih].Complete++
				} else {
					resp.Files[ih].Incomplete++
				}
				return true, nil
			})
			if err!=nil {
				log.Error("Walk","err", err)
				WriteResp(writer, ErrResponse{FailureReason: err.Error()}, false)
				return
			}
			jsonResp,err:=json.Marshal(resp)
			if err==nil {
				log.Info("scrape resp", "v", string(jsonResp))
			}else {
				log.Info("marshall scrape resp", "err", err)
			}

			WriteResp(writer, resp,false)
		} )
		m.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			log.Warn("404", "url", request.RequestURI)
		})

		log.Info("Listen1")
		go func() {
			err := http.ListenAndServe(":80", m)
			log.Error("error", "err", err)
		}()
		<-cmd.Context().Done()
		return nil
	},
}


type Tracker struct {
	db ethdb.Database
}

/*
/announce?compact=1
&downloaded=0
&event="started"
&info_hash=D%22%5C%80%F7%FD%12Z%EA%9B%F0%A5z%DA%AF%1F%A4%E1je
&left=0
&peer_id=-GT0002-9%EA%FB+%BF%B3%AD%DE%8Ae%D0%B7
&port=53631
&supportcrypto=1
&uploaded=0"
 */
type AnnounceReqWithTime struct {
	AnnounceReq
	UpdatedAt time.Time
}
type AnnounceReq struct {
	InfoHash []byte
	PeerID []byte
	RemoteAddr net.IP
	Port int
	Event string
	Uploaded int64
	Downloaded int64
	SupportCrypto bool
	Left int64
	Compact bool
}

type Peer struct {
	IP string
	Port int
	PeerID []byte
}

func (t *Tracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info("call","url", r.RequestURI)

	req,err:=ParseRequest(r)
	if err!=nil {
		log.Error("Parse request", "err", err)
		WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
		return
	}
	if err = ValidateReq(req); err!=nil {
		log.Error("Validate failed","err", err)
		WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
		return
	}

	toSave:=AnnounceReqWithTime{
		req,
		time.Now(),
	}
	peerBytes,err:=json.Marshal(toSave)
	if err!=nil {
		log.Error("Json marshal","err", err)
		WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
		return
	}

	key:=append(req.InfoHash, req.PeerID...)
	if req.Event==tracker.Stopped.String() {
		err = t.db.Delete(dbutils.SnapshotInfoBucket, key, nil)
		if err!=nil {
			log.Error("Json marshal","err", err)
			WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
			return
		}
	} else {
		if prevBytes,err:=t.db.Get(dbutils.SnapshotInfoBucket, key); err==nil&&len(prevBytes)>0 {
			prev:=new(AnnounceReqWithTime)
			err = json.Unmarshal(prevBytes,prev)
			if err!=nil {
				log.Error("Unable to unmarshall", "err", err)
			}
			if time.Now().Sub(prev.UpdatedAt) < time.Second*SoftLimit {
				//too early to update
				WriteResp(w, ErrResponse{FailureReason: "too early to update"}, req.Compact)
				return

			}
		} else if !errors.Is(err, ethdb.ErrKeyNotFound) && err!=nil {
			log.Error("get from db is return error", "err", err)
			WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
			return
		}
		err = t.db.Put(dbutils.SnapshotInfoBucket, key, peerBytes)
		if err!=nil {
			log.Error("db.Put","err", err)
			WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
			return
		}
	}

	resp := HttpResponse{
		Interval: DefaultInterval,
		TrackerId: trackerID,
	}

	err = t.db.Walk(dbutils.SnapshotInfoBucket, append(req.InfoHash, make([]byte, 20)...), 20*8, func(k, v []byte) (bool, error) {
		a:=AnnounceReqWithTime{}
		err = json.Unmarshal(v, &a)
		if err!=nil {
			log.Error("Fail to unmarshall", "k", common.Bytes2Hex(k), "err", err)
			//skip failed
			return true, nil
		}
		if time.Now().Sub(a.UpdatedAt) > 5*DisconnectInterval {
			log.Debug("Skipped requset", "peer", common.Bytes2Hex(a.PeerID), "last updated", a.UpdatedAt, "now", time.Now())
			return true, nil
		}
		if a.Left==0 {
			resp.Complete++
		} else {
			resp.Incomplete++
		}
		resp.Peers = append(resp.Peers, map[string]interface{}{
			"ip": a.RemoteAddr.String(),
			"peer id": a.PeerID,
			"port": a.Port,
		})
		return true, nil
	})
	if err!=nil {
		log.Error("Walk","err", err)
		WriteResp(w, ErrResponse{FailureReason: err.Error()}, req.Compact)
		return
	}
	jsonResp,err:=json.Marshal(resp)
	if err==nil {
		log.Info("announce resp", "v", string(jsonResp))
	} else {
		log.Info("marshall announce resp", "err", err)
	}

	WriteResp(w, resp,req.Compact)
}

func WriteResp(w http.ResponseWriter, res interface{}, compact bool)  {
	if _,ok:=res.(ErrResponse); ok {
		log.Error("Err", "err", res)
	}
	if compact {
		err := bencode.NewEncoder(w).Encode(res)
		if err!=nil {
			log.Error("Bencode encode", "err", err)
		}
	} else {
		err:=json.NewEncoder(w).Encode(res)
		if err!=nil {
			log.Error("Json marshal","err", err)
			return
		}
	}
}

func ParseRequest(r *http.Request) (AnnounceReq, error) {
	q:=r.URL.Query()

	var remoteAddr net.IP
	if strings.Contains(r.RemoteAddr, ":") {
		remoteAddr = net.ParseIP(strings.Split(r.RemoteAddr, ":")[0])
	} else {
		remoteAddr = net.ParseIP(r.RemoteAddr)
	}

	downloaded,err := strconv.ParseInt(q.Get("downloaded"),10, 64)
	if err!=nil {
		log.Warn("downloaded", "err", err)
		return AnnounceReq{}, fmt.Errorf("downloaded %v - %w",q.Get("downloaded"), err)
	}
	uploaded,err := strconv.ParseInt(q.Get("uploaded"),10, 64)
	if err!=nil {
		log.Warn("uploaded", "err", err)
		return AnnounceReq{}, fmt.Errorf("uploaded %v - %w",q.Get("uploaded"), err)
	}
	left,err := strconv.ParseInt(q.Get("left"),10, 64)
	if err!=nil {
		log.Warn("left", "err", err)
		return AnnounceReq{}, fmt.Errorf("left: %v - %w",q.Get("left"), err)
	}
	port,err := strconv.Atoi(q.Get("port"))
	if err!=nil {
		return AnnounceReq{}, fmt.Errorf("port: %v -  %w",q.Get("port"), err)
	}

	res:=AnnounceReq {
		InfoHash: []byte(q.Get("info_hash")),
		PeerID: []byte(q.Get("peer_id")),
		RemoteAddr: remoteAddr,
		Event: q.Get("event"),
		Compact: q.Get("compact")=="1",
		SupportCrypto: q.Get("supportcrypto")=="1",
		Downloaded: downloaded,
		Uploaded: uploaded,
		Left: left,
		Port: port,
	}
	return res, nil
}

func ValidateReq(req AnnounceReq) error {
	if len(req.InfoHash)!=20 {
		return errors.New("invalid infohash")
	}
	if len(req.PeerID)!=20 {
		return errors.New("invalid peer id")
	}
	if req.Port==0 {
		return errors.New("invalid port")
	}
	return nil
}

type HttpResponse struct {
	Interval      int32  `bencode:"interval" json:"interval"`
	TrackerId     string `bencode:"tracker id" json:"tracker_id"`
	Complete      int32  `bencode:"complete" json:"complete"`
	Incomplete    int32  `bencode:"incomplete" json:"incomplete"`
	Peers         []map[string]interface{}  `bencode:"peers" json:"peers"`
}
type ErrResponse struct {
	FailureReason string `bencode:"failure reason" json:"failure_reason"`
}
type ScrapeResponse struct {
	Files map[string]*ScrapeData `json:"files" bencode:"files"`
}

type ScrapeData struct {
	Complete int32 `bencode:"complete" json:"complete"`
	Downloaded int32 `json:"downloaded" bencode:"downloaded"`
	Incomplete  int32 `json:"incomplete" bencode:"incomplete"`
}