package function

import (
	"context"
	"fmt"
	"kis-flow/kis"
)

type KisFunctionC struct {
	BaseFunction
}

func (f *KisFunctionC) Call(ctx context.Context, flow kis.Flow) error {
	fmt.Printf("KisFunction_C, flow = %+v\n", flow)

	// TODO 调用具体的Function执行方法

	return nil
}
