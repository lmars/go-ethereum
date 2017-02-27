// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package network

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/network/kademlia"
	"github.com/pborman/uuid"
)

var (
	ErrStreamNotFound = errors.New("stream not found")
	ErrStreamExists   = errors.New("stream already exists")
	ErrStreamClosed   = errors.New("stream is closed")
)

type StreamID struct {
	Addr kademlia.Address
	Path string
}

func (s StreamID) String() string {
	return path.Join(s.Addr.String(), s.Path)
}

type StreamHandler struct {
	hive    *Hive
	mtx     sync.Mutex
	streams map[StreamID]*Stream
}

func NewStreamHandler(hive *Hive) *StreamHandler {
	return &StreamHandler{
		hive:    hive,
		streams: make(map[StreamID]*Stream),
	}
}

func (s *StreamHandler) StreamReader(id StreamID) (io.ReadCloser, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	stream, exists := s.streams[id]
	if !exists {
		// if the stream doesn't exist but we are the expected source
		// of the stream then return an error as there is no data to
		// stream to the client (if the stream gets created locally in
		// the future, the client can just try connecting again)
		if id.Addr == s.hive.Addr() {
			return nil, ErrStreamNotFound
		}

		stream = newStream(id, s)
		s.streams[id] = stream
		go s.connect(stream)
	}

	return stream.newClient()
}

func (s *StreamHandler) StreamWriter(path string) (io.WriteCloser, StreamID, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	id := StreamID{Addr: s.hive.Addr(), Path: path}
	if _, ok := s.streams[id]; ok {
		return nil, StreamID{}, ErrStreamExists
	}
	stream := newStream(id, s)
	s.streams[id] = stream
	return stream, id, nil
}

func (s *StreamHandler) connect(stream *Stream) (err error) {
	defer func() {
		// if we fail to send a connect message to a closer peer, we
		// aren't going to receive any data for the stream so just
		// close it
		if err != nil {
			log.Debug(fmt.Sprintf("StreamHandler.connect: error connecting stream %v: %s", stream.id, err))
			stream.Close()
		}
	}()

	// lookup closer peers
	peers := s.hive.getAddrPeers(stream.id.Addr, 0)
	log.Trace(fmt.Sprintf("StreamHandler.connect: %v - received %d peers from KΛÐΞMLIΛ...", stream.id, len(peers)))

	// if there are no closer peers or we are the closest peer, then we
	// have nowhere to send the connect message to so just return an error
	if len(peers) < 1 {
		return fmt.Errorf("no available peers closer to addr: %s", stream.id.Addr)
	}
	p := peers[0]
	if p.Addr() == s.hive.Addr() {
		return fmt.Errorf("no available peers closer to addr: %s", stream.id.Addr)
	}

	// send the connect message to the peer
	log.Trace(fmt.Sprintf("StreamHandler.connect: sending stream connect %v to peer [%v]", stream.id, p))
	return p.sendStreamConnect(stream.id)
}

func (s *StreamHandler) removeStream(stream *Stream) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.streams, stream.id)
}

// HandleStreamConnectMsg handles the receipt of a connect message from a
// downstream peer which instructs us to start sending it data for the given
// stream
func (s *StreamHandler) HandleStreamConnectMsg(req *streamConnectMsgData, p *peer) {
	client, err := s.StreamReader(req.ID)
	if err != nil {
		p.sendStreamError(req.ID, err)
		return
	}

	// start a goroutine to send the data read from the stream to the
	// downstream peer
	go func() {
		defer client.Close()
		buf := make([]byte, 4*1024)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				p.sendStreamData(req.ID, client.(*StreamClient).id, buf[:n])
			}
			if err != nil {
				p.sendStreamError(req.ID, err)
				return
			}
		}
	}()

}

// HandleStreamDisconnectMsg handles the receipt of a disconnect message from a
// downstream peer which instructs us to stop sending it data for the given
// stream (because they no longer have any clients reading from the stream)
func (s *StreamHandler) HandleStreamDisconnectMsg(req *streamDisconnectMsgData, p *peer) {
	s.mtx.Lock()
	stream, ok := s.streams[req.ID]
	s.mtx.Unlock()
	if !ok {
		return
	}

	stream.mtx.Lock()
	defer stream.mtx.Unlock()
	if client, ok := stream.clients[req.ClientID]; ok {
		client.Close()
		delete(stream.clients, client.id)
	}
}

func (s *StreamHandler) HandleStreamDataMsg(req *streamDataMsgData, p *peer) {
	s.mtx.Lock()
	stream, ok := s.streams[req.ID]
	s.mtx.Unlock()
	if !ok {
		// we have received data for a stream which we have no clients
		// for, so tell the peer to stop sending data
		p.sendStreamDisconnect(req.ID, req.ClientID)
		return
	}

	_, err := stream.Write(req.Data)
	if err != nil {
		// this error indicates that the stream has just been closed,
		// meaning all the clients have also been closed, so tell the
		// peer to stop sending data
		p.sendStreamDisconnect(req.ID, req.ClientID)
		return
	}
}

func (s *StreamHandler) HandleStreamErrorMsg(req *streamErrorMsgData, p *peer) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if stream, ok := s.streams[req.ID]; ok {
		stream.Close()
	}
}

type Stream struct {
	id      StreamID
	handler *StreamHandler
	mtx     sync.Mutex
	clients map[string]*StreamClient
	closed  bool
}

func newStream(id StreamID, handler *StreamHandler) *Stream {
	return &Stream{
		id:      id,
		handler: handler,
		clients: make(map[string]*StreamClient),
	}
}

// Write writes the data to all of the stream's clients
func (s *Stream) Write(p []byte) (n int, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// if the stream has been closed, all the clients will also have
	// been closed so just return an error
	if s.closed {
		return 0, ErrStreamClosed
	}

	// write the data to each client's write buffer, closing any clients
	// for which the write fails (which means the buffer is full, so either
	// the client is too slow or has gone away without telling us)
	for _, client := range s.clients {
		n, err := client.bufW.Write(p)
		if n > 0 {
			// flush the data in a goroutine so we don't hold the
			// mutex whilst waiting for the client to read from the
			// buffer
			go client.bufW.Flush()
		}
		if err != nil {
			client.Close()
			delete(s.clients, client.id)
		}
	}

	// TODO: consider closing the stream and sending a disconnect if there
	//       are no clients left and the stream source is not local

	// assume all data was written (dodgy clients shouldn't stop the source
	// from continuing to write data to the stream)
	return len(p), nil
}

// Close marks the stream as closed (so that any further writes are rejected),
// closes all the stream's clients and removes the stream from the handler
func (s *Stream) Close() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, client := range s.clients {
		client.Close()
	}
	s.handler.removeStream(s)
	return
}

func (s *Stream) newClient() (*StreamClient, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.closed {
		return nil, ErrStreamClosed
	}
	pipeR, pipeW := io.Pipe()
	client := &StreamClient{
		id:    uuid.New(),
		pipeR: pipeR,
		pipeW: pipeW,
		bufW:  bufio.NewWriter(pipeW),
	}
	s.clients[client.id] = client
	return client, nil
}

// StreamClient wraps an in-memory synchronous pipe with a buffer at the write
// end so that reads block waiting for incoming stream data, but writes are
// async and just fill the buffer (leading to write errors if the buffer fills
// giving us the chance to detect and disconnect slow or absent clients)
type StreamClient struct {
	id    string
	pipeR *io.PipeReader
	pipeW *io.PipeWriter
	bufW  *bufio.Writer
}

func (s *StreamClient) Read(p []byte) (n int, err error) {
	return s.pipeR.Read(p)
}

func (s *StreamClient) Close() error {
	return s.pipeW.Close()
}
