package main

import (
	"fmt"
	"masterdnsvpn-go/internal/enums"
	"masterdnsvpn-go/internal/vpnproto"
)

func main() {
    for pt := 0; pt < 256; pt++ {
        opts := vpnproto.BuildOptions{PacketType: uint8(pt)}
        _, err := vpnproto.BuildRaw(opts)
        if err != nil {
            name := enums.PacketTypeName(uint8(pt))
            if name != "UNKNOWN" {
                fmt.Printf("PacketType 0x%02X (%s) is INVALID\n", pt, name)
            }
        }
    }
}
