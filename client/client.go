package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func bePutU64(bs []byte, u64 uint64) {
	binary.BigEndian.PutUint64(bs, u64)
}

func beGetU64(bs []byte) uint64 {
	return binary.BigEndian.Uint64(bs)
}

func Assert(b bool) {
	if b {
	} else {
		panic("unexpected")
	}
}

func echo(conn net.Conn) {
	r := bufio.NewReader(conn)
	header := make([]byte, 8)
	close := func() {
		err := conn.Close()
		if err != nil {
			log.Println("tcp handler: close", err)
			return
		}
	}
	defer close()
	for {
		ct, err := io.ReadFull(r, header)
		if err != nil {
			log.Println("tcp handler: read header", err)
			return
		}
		Assert(ct == len(header))
		szToRead := beGetU64(header)
		Assert(szToRead < 1024*1024*1024)
		bodyBs := make([]byte, (int)(szToRead))
		ct, err = io.ReadFull(r, bodyBs)
		if err != nil {
			log.Println("tcp handler: read body", err, "szToRead", szToRead)
			return
		}
		Assert(ct == int(szToRead))
		bePutU64(header, szToRead)
		n, err := conn.Write(header)
		if err != nil {
			log.Println("tcp handler: write header", err, "szToRead", szToRead)
			return
		}
		Assert(n == len(header))
	}
}

type Resp struct {
	msg string
}

type Req struct {
	bs    []byte
	retCh chan *Resp
}

func startNewTcpClientDaemon(addr string) chan *Req {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalln("startNewTcpClientDaemon net.Dial", err, "addr")
	}
	rcvReqCh := make(chan *Req, 1024)
	handler := func() {
		header := make([]byte, 8)
		response := bufio.NewReader(conn)
		for {
			req := <-rcvReqCh
			Assert(len(req.bs) > 0)
			bePutU64(header, uint64(len(req.bs)))
			n, err := conn.Write(header)
			if err != nil {
				log.Fatalln("startNewTcpClientDaemon conn.Write header", err, "addr")
			}
			Assert(n == len(header))
			n, err = conn.Write(req.bs)
			if err != nil {
				log.Fatalln("startNewTcpClientDaemon conn.Write body", err, "addr")
			}
			Assert(n == len(req.bs))
			n, err = io.ReadFull(response, header)
			if err != nil {
				log.Fatalln("startNewTcpClientDaemon conn.Read header", err, "addr")
			}
			Assert(n == len(header))
			if beGetU64(header) != uint64(len(req.bs)) {
				log.Fatalln("startNewTcpClientDaemon beGetU64(header) != uint64(len(req.bs))", beGetU64(header), len(req.bs), "addr")
			}
			req.retCh <- &Resp{
				"ok",
			}
		}
	}
	go handler()
	return rcvReqCh
}

func main() {
	msgBodySize, err := strconv.Atoi(os.Args[1])
	Assert(err == nil)
	fmt.Println("msgBodySize", msgBodySize)
	addrs := []string{"172.31.43.184:3198", "172.31.41.132:3198"}
	//addrs := []string{"192.168.0.110:3198", "192.168.0.111:3198", "192.168.0.112:3198"}
	var tcpClientDaemonsCtl []chan *Req
	for _, addr := range addrs {
		tcpClientDaemonsCtl = append(tcpClientDaemonsCtl, startNewTcpClientDaemon(addr))
	}
	//
	msgBody := make([]byte, msgBodySize)
	for {
		fmt.Println("---->")
		t0 := time.Now()
		retCh := make(chan *Resp, len(tcpClientDaemonsCtl))
		for _, ctl := range tcpClientDaemonsCtl {
			ctl <- &Req{
				bs:    msgBody,
				retCh: retCh,
			}
		}
		for idx := range tcpClientDaemonsCtl {
			resp := <-retCh
			fmt.Println("get one ret", resp.msg, "idx", idx, time.Since(t0))
		}
		time.Sleep(time.Second)
	}
	//
}
