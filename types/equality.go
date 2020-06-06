package types

func EqualValues(a, b *Value) bool {
	if len(a.Changes) != len(b.Changes) {
		return false
	}
	for i := range a.Changes {
		if a.Changes[i].Type != b.Changes[i].Type {
			return false
		}
		if a.Changes[i].Node.ID != b.Changes[i].Node.ID {
			return false
		}
	}
	return true
}
