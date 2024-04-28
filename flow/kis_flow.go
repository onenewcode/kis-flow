package flow

import (
	"context"
	"errors"
	"fmt"
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/function"
	"kis-flow/id"
	"kis-flow/kis"
	"kis-flow/log"
	"sync"
)

// KisFlow 用于贯穿整条流式计算的上下文环境
type KisFlow struct {
	// 基础信息
	Id   string                // Flow的分布式实例ID(用于KisFlow内部区分不同实例)
	Name string                // Flow的可读名称
	Conf *config.KisFlowConfig // Flow配置策略

	// Function列表
	Funcs          map[string]kis.Function // 当前flow拥有的全部管理的全部Function对象, key: FunctionID
	FlowHead       kis.Function            // 当前Flow所拥有的Function列表表头
	FlowTail       kis.Function            // 当前Flow所拥有的Function列表表尾
	flock          sync.RWMutex            // 管理链表插入读写的锁
	ThisFunction   kis.Function            // Flow当前正在执行的KisFunction对象
	ThisFunctionId string                  // 当前执行到的Function ID (策略配置ID)
	PrevFunctionId string                  // 当前执行到的Function 上一层FunctionID(策略配置ID)

	// Function列表参数
	funcParams map[string]config.FParam // flow在当前Function的自定义固定配置参数,Key:function的实例NsID, value:FParam
	fplock     sync.RWMutex             // 管理funcParams的读写锁
	// ++++++++ 数据 ++++++++++
	buffer common.KisRowArr  // 用来临时存放输入字节数据的内部Buf, 一条数据为interface{}, 多条数据为[]interface{} 也就是KisBatch
	data   common.KisDataMap // 流式计算各个层级的数据源
	inPut  common.KisRowArr  // 当前Function的计算输入数据
}

// Link 将Function链接到Flow中
// fConf: 当前Function策略
// fParams: 当前Flow携带的Function动态参数
func (flow *KisFlow) Link(fConf *config.KisFuncConfig, fParams config.FParam) error {
	// 创建Function
	f := function.NewKisFunction(flow, fConf)

	// Flow 添加 Function
	if err := flow.appendFunc(f, fParams); err != nil {
		return err
	}

	return nil
}

// appendFunc 将Function添加到Flow中, 链表操作
func (flow *KisFlow) appendFunc(function kis.Function, fParam config.FParam) error {

	if function == nil {
		return errors.New("AppendFunc append nil to List")
	}

	flow.flock.Lock()
	defer flow.flock.Unlock()

	if flow.FlowHead == nil {
		// 首次添加节点
		flow.FlowHead = function
		flow.FlowTail = function

		function.SetN(nil)
		function.SetP(nil)

	} else {
		// 将function插入到链表的尾部
		function.SetP(flow.FlowTail)
		function.SetN(nil)

		flow.FlowTail.SetN(function)
		flow.FlowTail = function
	}

	//将Function ID 详细Hash对应关系添加到flow对象中
	flow.Funcs[function.GetId()] = function

	//先添加function 默认携带的Params参数
	params := make(config.FParam)
	for key, value := range function.GetConfig().Option.Params {
		params[key] = value
	}

	//再添加flow携带的function定义参数(重复即覆盖)
	for key, value := range fParam {
		params[key] = value
	}

	// 将得到的FParams存留在flow结构体中，用来function业务直接通过Hash获取
	// key 为当前Function的KisId，不用Fid的原因是为了防止一个Flow添加两个相同策略Id的Function
	flow.funcParams[function.GetId()] = params

	return nil
}

// Run 启动KisFlow的流式计算, 从起始Function开始执行流
func (flow *KisFlow) Run(ctx context.Context) error {

	var fn kis.Function

	fn = flow.FlowHead

	if flow.Conf.Status == int(common.FlowDisable) {
		//flow被配置关闭
		return nil
	}

	// ========= 数据流 新增 ===========
	// 因为此时还没有执行任何Function, 所以PrevFunctionId为FirstVirtual 因为没有上一层Function
	flow.PrevFunctionId = common.FunctionIdFirstVirtual

	// 提交数据流原始数据
	if err := flow.commitSrcData(ctx); err != nil {
		return err
	}
	// ========= 数据流 新增 ===========

	//流式链式调用
	for fn != nil {

		// ========= 数据流 新增 ===========
		// flow记录当前执行到的Function 标记
		fid := fn.GetId()
		flow.ThisFunction = fn
		flow.ThisFunctionId = fid

		// 得到当前Function要处理与的源数据
		if inputData, err := flow.getCurData(); err != nil {
			log.Logger().ErrorFX(ctx, "flow.Run(): getCurData err = %s\n", err.Error())
			return err
		} else {
			flow.inPut = inputData
		}
		// ========= 数据流 新增 ===========

		if err := fn.Call(ctx, flow); err != nil {
			//Error
			return err
		} else {
			//Success

			// ========= 数据流 新增 ===========
			if err := flow.commitCurData(ctx); err != nil {
				return err
			}

			// 更新上一层FuncitonId游标
			flow.PrevFunctionId = flow.ThisFunctionId
			// ========= 数据流 新增 ===========

			fn = fn.Next()
		}
	}

	return nil
}

// CommitRow 提交Flow数据, 一行数据，如果是批量数据可以提交多次
func (flow *KisFlow) CommitRow(row interface{}) error {

	flow.buffer = append(flow.buffer, row)

	return nil
}

// Input 得到flow当前执行Function的输入源数据
func (flow *KisFlow) Input() common.KisRowArr {
	return flow.inPut
}

// commitSrcData 提交当前Flow的数据源数据, 表示首次提交当前Flow的原始数据源
// 将flow的临时数据buffer，提交到flow的data中,(data为各个Function层级的源数据备份)
// 会清空之前所有的flow数据
func (flow *KisFlow) commitSrcData(ctx context.Context) error {

	// 制作批量数据batch
	dataCnt := len(flow.buffer)
	batch := make(common.KisRowArr, 0, dataCnt)

	for _, row := range flow.buffer {
		batch = append(batch, row)
	}

	// 清空之前所有数据
	flow.clearData(flow.data)

	// 首次提交，记录flow原始数据
	// 因为首次提交，所以PrevFunctionId为FirstVirtual 因为没有上一层Function
	flow.data[common.FunctionIdFirstVirtual] = batch

	// 清空缓冲Buf
	flow.buffer = flow.buffer[0:0]

	log.Logger().DebugFX(ctx, "====> After CommitSrcData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}

// getCurData 获取flow当前Function层级的输入数据
func (flow *KisFlow) getCurData() (common.KisRowArr, error) {
	if flow.PrevFunctionId == "" {
		return nil, errors.New(fmt.Sprintf("flow.PrevFunctionId is not set"))
	}

	if _, ok := flow.data[flow.PrevFunctionId]; !ok {
		return nil, errors.New(fmt.Sprintf("[%s] is not in flow.data", flow.PrevFunctionId))
	}

	return flow.data[flow.PrevFunctionId], nil
}

// commitCurData 提交Flow当前执行Function的结果数据
func (flow *KisFlow) commitCurData(ctx context.Context) error {

	//判断本层计算是否有结果数据,如果没有则退出本次Flow Run循环
	if len(flow.buffer) == 0 {
		return nil
	}

	// 制作批量数据batch
	batch := make(common.KisRowArr, 0, len(flow.buffer))

	//如果strBuf为空，则没有添加任何数据
	for _, row := range flow.buffer {
		batch = append(batch, row)
	}

	//将本层计算的缓冲数据提交到本层结果数据中
	flow.data[flow.ThisFunctionId] = batch

	//清空缓冲Buf
	flow.buffer = flow.buffer[0:0]

	log.Logger().DebugFX(ctx, " ====> After commitCurData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}

// ClearData 清空flow所有数据
func (flow *KisFlow) clearData(data common.KisDataMap) {
	for k := range data {
		delete(data, k)
	}
}

// NewKisFlow 创建一个KisFlow.
func NewKisFlow(conf *config.KisFlowConfig) kis.Flow {
	flow := new(KisFlow)
	// 实例Id
	flow.Id = id.KisID(common.KisIdTypeFlow)

	// 基础信息
	flow.Name = conf.FlowName
	flow.Conf = conf

	// Function列表
	flow.Funcs = make(map[string]kis.Function)
	flow.funcParams = make(map[string]config.FParam)

	// ++++++++ 数据data +++++++
	flow.data = make(common.KisDataMap)

	return flow
}
