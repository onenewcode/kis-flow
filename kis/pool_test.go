package kis

import (
	"context"
	"fmt"
)

func funcName1Handler(ctx context.Context, flow Flow) error {
	fmt.Println("---> Call funcName1Handler ----")

	for index, row := range flow.Input() {
		// 打印数据
		str := fmt.Sprintf("In FuncName = %s, FuncId = %s, row = %s", flow.GetThisFuncConf().FName, flow.GetThisFunction().GetId(), row)
		fmt.Println(str)

		// 计算结果数据
		resultStr := fmt.Sprintf("data from funcName[%s], index = %d", flow.GetThisFuncConf().FName, index)

		// 提交结果数据
		_ = flow.CommitRow(resultStr)
	}

	return nil
}

func funcName2Handler(ctx context.Context, flow Flow) error {

	for _, row := range flow.Input() {
		str := fmt.Sprintf("In FuncName = %s, FuncId = %s, row = %s", flow.GetThisFuncConf().FName, flow.GetThisFunction().GetId(), row)
		fmt.Println(str)
	}

	return nil
}
