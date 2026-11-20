package kolotransport_test

import (
	"fmt"

	kolotransport "github.com/ciranda-dev/kolo-transport"
)

func ExampleMaxCode() {
	fmt.Println(kolotransport.MaxCode)
	// Output: 4611686018427387903
}
