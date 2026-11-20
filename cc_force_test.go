package kolotransport

import "os"

// forcedCC is the controller the whole suite runs with when KOLO_TRANSPORT_TEST_CC is set, as "bbr" for the BBR run
// of the gate. It sets the default of Config.CongestionControl, so a test that needs the production default skips.
var forcedCC = os.Getenv("KOLO_TRANSPORT_TEST_CC")

func init() {
	if forcedCC != "" {
		defaultCongestionControl = forcedCC
	}
}
