package model

// Operation type
type Operation uint8

const (
	// Insert operation
	Insert Operation = iota
	// Delete operation
	Delete
	// Update operation
	Update
	// Begin transaction
	Begin
	// Commit transaction
	Commit
	// Unknow operation
	Unknow
)

func (o Operation) String() string {
	switch o {
	case Insert:
		return "INSERT"
	case Delete:
		return "DELETE"
	case Update:
		return "UPDATE"
	case Begin:
		return "BEGIN"
	case Commit:
		return "COMMIT"
	}

	return "UNKNOW"
}
