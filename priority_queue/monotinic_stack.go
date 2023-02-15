package priorityqueue

type stack struct {
	/*
		[] - slice with intervals
		[2] - array with start and end intervals
	*/
	idx [][2]int
}

func newStack() *stack {
	return &stack{
		idx: make([][2]int, 0, 10),
	}
}

func (st *stack) add(idx int) {
	/*
		we have to store the beginning of the range + the last
	*/

	// st.intervals[len(st.intervals)-1][1] - last seen interval

	// firstly seen
	if len(st.idx) == 0 {
		st.idx = append(st.idx, [2]int{
			idx, idx,
		})
		return
	}

	// last[1] is a previously added element
	if st.idx[len(st.idx)-1][1]+1 == idx {
		st.idx[len(st.idx)-1][1] = idx
		return
	}

	st.idx = append(st.idx, [2]int{
		idx, idx,
	})
}

func (st *stack) indices() [][2]int {
	return st.idx
}

func (st *stack) clear() {
	st.idx = make([][2]int, 0, 10)
}
