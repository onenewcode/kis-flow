package kis

// Action 是指在执行Function的时候，同时可以控制Flow的调度逻辑
// Action KisFlow执行流程Actions
type Action struct {
	// DataReuse 是否复用上层Function数据
	DataReuse bool
	// 默认Next()为如果本层Function计算结果为0条数据，之后Function将不会继续执行
	// ForceEntryNext 为忽略上述默认规则，没有数据强制进入下一层Function
	ForceEntryNext bool
	// ++++++++++
	// JumpFunc 跳转到指定Function继续执行
	JumpFunc string
	// Abort 终止Flow的执行
	Abort bool
}

// ActionFunc KisFlow Functional Option 类型
type ActionFunc func(ops *Action)

// LoadActions 加载Actions，依次执行ActionFunc操作函数
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

// ActionAbort 终止Flow的执行
func ActionAbort(action *Action) {
	action.Abort = true
}

// ActionDataReuse Next复用上层Function数据Option
func ActionDataReuse(act *Action) {
	act.DataReuse = true
}
