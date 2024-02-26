package column

type TypeProcessor interface {
	Type() Type
	Load(size int, payload []byte) (Column, error)
}
