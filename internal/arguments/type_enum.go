package arguments

// Type represents a specific type of argument
type Type int

const (
	typeCount Type = iota
	typeUInt
	typeBool
	typeString
	typeStringArray
	typePath
	typeUnknown
)

var (
	typeToString = map[Type]string{
		typeCount:       "typeCount",
		typeUInt:        "typeUInt",
		typeBool:        "typeBool",
		typeString:      "typeString",
		typeStringArray: "typeStringArray",
		typePath:        "typePath",
		typeUnknown:     "typeUnknown",
	}
)

func (at Type) String() string {
	value, exists := typeToString[at]
	if !exists {
		return typeUnknown.String()
	}
	return value
}
