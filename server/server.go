package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
	"time"
)

var addr = "0.0.0.0:3198"

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

var glFile *os.File

func writeAndSyncData(bs []byte) time.Duration {
	t0 := time.Now()
	f := glFile
	n, err := f.WriteAt(bs, 0)
	if err != nil {
		log.Fatalln("writeAndSyncData", err)
	}
	Assert(n == len(bs))
	err = syscall.Fdatasync(int(f.Fd()))
	Assert(err == nil)
	return time.Since(t0)
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
		// simulate fdatasync
		tDiff := writeAndSyncData(bodyBs)
		fmt.Println("writeAndSyncData sz", len(bodyBs), tDiff)
		//time.Sleep(time.Millisecond)
		//
		bePutU64(header, szToRead)
		n, err := conn.Write(header)
		if err != nil {
			log.Println("tcp handler: write header", err, "szToRead", szToRead)
			return
		}
		Assert(n == len(header))
	}
}

func main() {
	f, err := os.OpenFile("paxos.sim.paxos.log", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	glFile = f

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("ERROR", err)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("ERROR", err)
			continue
		}
		go echo(conn)
	}
}
