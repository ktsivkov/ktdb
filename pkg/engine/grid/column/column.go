package column

type Column interface {
	Type() Type
	Bytes(size int) ([]byte, error)
}
