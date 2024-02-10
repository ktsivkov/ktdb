package sys

import "unsafe"

// IntByteSize represents the size of a typical int in bytes depending on the system's architecture.
const IntByteSize = int(unsafe.Sizeof(0))
