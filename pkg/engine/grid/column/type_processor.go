package column

type TypeProcessor interface {
	Type() Type
	Load(size int64, payload []byte) (Column, error)
}
