package deduplication

type Result int

const (
	New Result = iota
	Duplicate
	Conflict
)

// Classify сравнивает хеши события с тем же ключом идемпотентности.
func Classify(existingHash, incomingHash string) Result {
	if existingHash == incomingHash {
		return Duplicate
	}
	return Conflict
}
