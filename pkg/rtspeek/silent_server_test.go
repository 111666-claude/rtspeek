package rtspeek

import (
	"testing"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
)

// silentServer answers DESCRIBE/SETUP/PLAY but never sends an RTP packet, so
// an analyze window must end in an empty-window event.
type silentServer struct {
	srv    *gortsplib.Server
	stream *gortsplib.ServerStream
}

func startSilentServer(t *testing.T, addr string) *silentServer {
	t.Helper()
	media := &description.Media{
		Type: description.MediaTypeVideo,
		Formats: []format.Format{
			&format.H264{PayloadTyp: 96, PacketizationMode: 1},
		},
	}
	s := &silentServer{}
	s.srv = &gortsplib.Server{RTSPAddress: addr, Handler: s}
	if err := s.srv.Start(); err != nil {
		t.Fatalf("silent server start: %v", err)
	}
	s.stream = &gortsplib.ServerStream{Server: s.srv, Desc: &description.Session{Medias: []*description.Media{media}}}
	if err := s.stream.Initialize(); err != nil {
		t.Fatalf("silent stream init: %v", err)
	}
	return s
}

func (s *silentServer) Close() {
	s.stream.Close()
	s.srv.Close()
}

func (s *silentServer) OnDescribe(*gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	return &base.Response{StatusCode: base.StatusOK}, s.stream, nil
}

func (s *silentServer) OnSetup(*gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	return &base.Response{StatusCode: base.StatusOK}, s.stream, nil
}

func (s *silentServer) OnPlay(*gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	return &base.Response{StatusCode: base.StatusOK}, nil
}
