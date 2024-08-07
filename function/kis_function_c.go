package function

import (
	"context"
	"kis-flow/kis"
	"kis-flow/log"
)

type KisFunctionC struct {
	BaseFunction
}

func NewKisFunctionC() kis.Function {
	f := new(KisFunctionC)

	// 初始化metaData
	f.metaData = make(map[string]interface{})

	return f
}

func (f *KisFunctionC) Call(ctx context.Context, flow kis.Flow) error {
	log.Logger().DebugF("KisFunctionC, flow = %+v\n", flow)

	// 通过KisPool 路由到具体的执行计算Function中
	if err := kis.Pool().CallFunction(ctx, f.Config.FName, flow); err != nil {
		log.Logger().ErrorFX(ctx, "Function Called Error err = %s\n", err)
		return err
	}

	return nil
}
