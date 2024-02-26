package column

type Column interface {
	Type() Type
	Bytes(size int64) ([]byte, error)
}
