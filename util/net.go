package util

import (
  "strconv"
  "strings"
)

func Inet_addr(ipaddr string) uint32 {
  var (
    ip                 = strings.Split(ipaddr, ".")
    ip1, ip2, ip3, ip4 uint64
    ret                uint32
  )
  ip1, _ = strconv.ParseUint(ip[0], 10, 8)
  ip2, _ = strconv.ParseUint(ip[1], 10, 8)
  ip3, _ = strconv.ParseUint(ip[2], 10, 8)
  ip4, _ = strconv.ParseUint(ip[3], 10, 8)
  ret = uint32(ip4)<<24 + uint32(ip3)<<16 + uint32(ip2)<<8 + uint32(ip1)
  return ret
}
