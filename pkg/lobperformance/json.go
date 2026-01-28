package lobperformance

import "encoding/json"

const minJSize uint = 48

// JObj is a placeholder for generating some JSON formatted text
type JObj struct {
	ID    int      `json:"id"`
	Name  string   `json:"name"`
	Items []string `json:"items"`
}

func newGeneratedJObj(chunkSize uint, size uint) JObj {
	step := chunkSize + 3
	o := JObj{
		ID:   randomInt(),
		Name: randomString(chunkSize),
	}
	rest := size - minJSize - chunkSize
	for {
		if rest < step {
			o.Items = append(o.Items, randomString(rest))
			return o
		}
		o.Items = append(o.Items, randomString(chunkSize))
		rest -= step
	}
}

func (j JObj) String() (string, error) {
	bytes, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
