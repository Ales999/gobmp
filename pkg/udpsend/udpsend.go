package udpsend

import (
	"encoding/json"
	"net"
	"strings"

	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/pub"
)

type msgOut struct {
	MsgType int    `json:"msg_type,omitempty"`
	MsgHash string `json:"msg_hash,omitempty"`
	Msg     []byte `json:"msg_data,omitempty"`
}

type pubudpsend struct {
	nc       *net.UDPConn
	HostName string
	Port     string
}

func (u *pubudpsend) PublishMessage(msgType int, msgHash []byte, msg []byte) error {
	m := msgOut{
		MsgType: msgType,
		MsgHash: string(msgHash),
		Msg:     msg,
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	// Send message by UDP
	_, err = u.nc.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (u *pubudpsend) Stop() {
	u.nc.Close()
}

func NewUdpSend(host string, port string) (pub.Publisher, error) {

	glog.Infof("Initializing UDP producer client")
	var toSend strings.Builder
	toSend.WriteString(host)
	toSend.WriteString(":")
	toSend.WriteString(port)

	addr, err := net.ResolveUDPAddr("udp4", toSend.String())
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return nil, err
	}

	udp := pubudpsend{
		nc:       conn,
		HostName: host,
		Port:     port,
	}
	return &udp, nil

}
