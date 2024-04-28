package flow

import (
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/id"
	"kis-flow/kis"
)

// KisFlow 用于贯穿整条流式计算的上下文环境
type KisFlow struct {
	Id   string
	Name string
	// TODO
}

// TODO for test
// NewKisFlow 创建一个KisFlow.
func NewKisFlow(conf *config.KisFlowConfig) kis.Flow {
	flow := new(KisFlow)

	// 基础信息
	flow.Id = id.KisID(common.KisIdTypeFlow)
	flow.Name = conf.FlowName

	return flow
}
