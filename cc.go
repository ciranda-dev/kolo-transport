package kolotransport

import (
	"github.com/ciranda-dev/kolo-transport/internal/bbr"
	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
)

// useCongestionControl switches qc to the controller cfg names; Reno is quic-go's own default and needs no change.
// Both switches survive a path migration, which would otherwise bring Reno back.
func useCongestionControl(qc *quic.Conn, cfg Config) {
	switch cfg.CongestionControl {
	case congestionCubic:
		qc.UseCubic()
	case congestionBBR:
		qc.SetCongestionControlFactory(newBBR)
	}
}

func newBBR(initialSize congestion.ByteCount) congestion.CongestionControl {
	return bbr.NewBbrSender(bbr.DefaultClock{}, initialSize)
}
