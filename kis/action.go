package kis

// Action 是指在执行Function的时候，同时可以控制Flow的调度逻辑
// Action KisFlow执行流程Actions
type Action struct {
	// DataReuse 是否复用上层Function数据
	DataReuse bool
	// 默认Next()为如果本层Function计算结果为0条数据，之后Function将不会继续执行
	// ForceEntryNext 为忽略上述默认规则，没有数据强制进入下一层Function
	ForceEntryNext bool
	// JumpFunc 跳转到指定Function继续执行
	JumpFunc string
	// Abort 终止Flow的执行
	Abort bool
}

// ActionFunc is the type for KisFlow Functional Option.
type ActionFunc func(ops *Action)

// LoadActions loads Actions and sequentially executes the ActionFunc operations.
func LoadActions(acts []ActionFunc) Action {
	action := Action{}

	if acts == nil {
		return action
	}

	for _, act := range acts {
		act(&action)
	}

	return action
}

// ActionDataReuse sets the option for reusing data from the upper Function.
func ActionDataReuse(act *Action) {
	act.DataReuse = true
}

// ActionForceEntryNext sets the option to forcefully enter the next layer.
func ActionForceEntryNext(act *Action) {
	act.ForceEntryNext = true
}

// ActionJumpFunc returns an ActionFunc function and sets the funcName to Action.JumpFunc.
// (Note: This can easily lead to Flow loop calls, causing an infinite loop.)
func ActionJumpFunc(funcName string) ActionFunc {
	return func(act *Action) {
		act.JumpFunc = funcName
	}
}

// ActionAbort terminates the execution of the Flow.
func ActionAbort(action *Action) {
	action.Abort = true
}
