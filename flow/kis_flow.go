package flow

import (
	"context"
	"errors"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/conn"
	"kis-flow/function"
	"kis-flow/id"
	"kis-flow/kis"
	"kis-flow/log"
	"kis-flow/metrics"
	"sync"
	"time"
)

// KisFlow 用于贯穿整条流式计算的上下文环境
type KisFlow struct {
	// 基础信息
	Id   string                // Flow的分布式实例ID(用于KisFlow内部区分不同实例)
	Name string                // Flow的可读名称
	Conf *config.KisFlowConfig // Flow配置策略

	// Function列表
	Funcs          map[string]kis.Function // 当前flow拥有的全部管理的全部Function对象, key: FunctionName
	FlowHead       kis.Function            // 当前Flow所拥有的Function列表表头
	FlowTail       kis.Function            // 当前Flow所拥有的Function列表表尾
	flock          sync.RWMutex            // 管理链表插入读写的锁
	ThisFunction   kis.Function            // Flow当前正在执行的KisFunction对象
	ThisFunctionId string                  // 当前执行到的Function ID
	PrevFunctionId string                  // 当前执行到的Function 上一层FunctionID

	// Function列表参数
	funcParams map[string]config.FParam // flow在当前Function的自定义固定配置参数,Key:function的实例KisID, value:FParam
	fplock     sync.RWMutex             // 管理funcParams的读写锁

	// 数据
	buffer common.KisRowArr  // 用来临时存放输入字节数据的内部Buf, 一条数据为interface{}, 多条数据为[]interface{} 也就是KisBatch
	data   common.KisDataMap // 流式计算各个层级的数据源
	inPut  common.KisRowArr  // 当前Function的计算输入数据
	abort  bool              // 是否中断Flow
	action kis.Action        // 当前Flow所携带的Action动作

	// flow的本地缓存
	cache *cache.Cache // Flow流的临时缓存上线文环境

	// flow的metaData
	metaData map[string]interface{} // Flow的自定义临时数据
	mLock    sync.RWMutex           // 管理metaData的读写锁
}

// Link 将Function链接到Flow中
// fConf: 当前Function策略
// fParams: 当前Flow携带的Function动态参数
func (flow *KisFlow) Link(fConf *config.KisFuncConfig, fParams config.FParam) error {
	// 创建Function实例
	f := function.NewKisFunction(flow, fConf)
	if fConf.Option.CName != "" {
		// 当前Function有Connector关联，需要初始化Connector实例

		// 获取Connector配置
		connConfig, err := fConf.GetConnConfig()
		if err != nil {
			panic(err)
		}

		// 创建Connector对象
		connector := conn.NewKisConnector(connConfig)

		// 初始化Connector, 执行Connector Init 方法
		if err = connector.Init(); err != nil {
			panic(err)
		}

		// 关联Function实例和Connector实例关系
		_ = f.AddConnector(connector)
	}
	// Flow 添加 Function
	if err := flow.appendFunc(f, fParams); err != nil {
		return err
	}
	// 初始化本地缓存
	flow.cache = cache.New(cache.NoExpiration, common.DeFaultFlowCacheCleanUp*time.Minute)
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

	//将Function Name 详细Hash对应关系添加到flow对象中
	flow.Funcs[function.GetConfig().FName] = function

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
	flow.abort = false

	if flow.Conf.Status == int(common.FlowDisable) {
		// flow被配置关闭
		return nil
	}

	// Metrics
	var funcStart time.Time
	var flowStart time.Time

	// 因为此时还没有执行任何Function, 所以PrevFunctionId为FirstVirtual 因为没有上一层Function
	flow.PrevFunctionId = common.FunctionIdFirstVirtual

	// 提交数据流原始数据
	if err := flow.commitSrcData(ctx); err != nil {
		return err
	}

	// 判断是否开启监控 Metrics
	if config.GlobalConfig.EnableProm == true {
		// 统计Flow的调度次数
		metrics.Metrics.FlowScheduleCntsToTal.WithLabelValues(flow.Name).Inc()
		// 统计Flow的执行消耗时长
		flowStart = time.Now()
	}

	// 流式链式调用
	for fn != nil && flow.abort == false {

		// flow记录当前执行到的Function 标记
		fid := fn.GetId()
		flow.ThisFunction = fn
		flow.ThisFunctionId = fid

		fName := fn.GetConfig().FName
		fMode := fn.GetConfig().FMode

		if config.GlobalConfig.EnableProm == true {
			// 统计Function调度次数
			metrics.Metrics.FuncScheduleCntsTotal.WithLabelValues(fName, fMode).Inc()

			// 统计Function 耗时 记录开始时间
			funcStart = time.Now()
		}

		// 得到当前Function要处理与的源数据
		if inputData, err := flow.getCurData(); err != nil {
			log.Logger().ErrorFX(ctx, "flow.Run(): getCurData err = %s\n", err.Error())
			return err
		} else {
			flow.inPut = inputData
		}

		if err := fn.Call(ctx, flow); err != nil {
			// Error
			return err
		} else {
			// Success
			fn, err = flow.dealAction(ctx, fn)
			if err != nil {
				return err
			}

			// 统计Function 耗时
			if config.GlobalConfig.EnableProm == true {
				// Function消耗时间
				duration := time.Since(funcStart)

				// 统计当前Function统计指标,做时间统计
				metrics.Metrics.FunctionDuration.With(
					prometheus.Labels{
						common.LABEL_FUNCTION_NAME: fName,
						common.LABEL_FUNCTION_MODE: fMode}).Observe(duration.Seconds() * 1000)
			}

		}
	}

	// Metrics
	if config.GlobalConfig.EnableProm == true {
		// 统计Flow执行耗时
		duration := time.Since(flowStart)
		metrics.Metrics.FlowDuration.WithLabelValues(flow.Name).Observe(duration.Seconds() * 1000)
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
	// 首次提交数据源数据，进行统计数据总量
	if config.GlobalConfig.EnableProm == true {
		// 统计数据总量 Metrics.DataTota 指标累计加1
		metrics.Metrics.DataTotal.Add(float64(dataCnt))
	}
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

	// 判断本层计算是否有结果数据,如果没有则退出本次Flow Run循环
	if len(flow.buffer) == 0 {
		flow.abort = true
		return nil
	}

	// 制作批量数据batch
	batch := make(common.KisRowArr, 0, len(flow.buffer))

	// 如果strBuf为空，则没有添加任何数据
	for _, row := range flow.buffer {
		batch = append(batch, row)
	}

	// 将本层计算的缓冲数据提交到本层结果数据中
	flow.data[flow.ThisFunctionId] = batch

	// 清空缓冲Buf
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
func (flow *KisFlow) GetName() string {
	return flow.Name
}

func (flow *KisFlow) GetThisFunction() kis.Function {
	return flow.ThisFunction
}

func (flow *KisFlow) GetThisFuncConf() *config.KisFuncConfig {
	return flow.ThisFunction.GetConfig()
}

// GetConnector 得到当前正在执行的Function的Connector
func (flow *KisFlow) GetConnector() (kis.Connector, error) {
	if conn := flow.ThisFunction.GetConnector(); conn != nil {
		return conn, nil
	} else {
		return nil, errors.New("GetConnector(): Connector is nil")
	}
}

// GetConnConf 得到当前正在执行的Function的Connector的配置
func (flow *KisFlow) GetConnConf() (*config.KisConnConfig, error) {
	if conn := flow.ThisFunction.GetConnector(); conn != nil {
		return conn.GetConfig(), nil
	} else {
		return nil, errors.New("GetConnConf(): Connector is nil")
	}
}

func (flow *KisFlow) GetConfig() *config.KisFlowConfig {
	return flow.Conf
}

// GetFuncConfigByName 得到当前Flow的配置
func (flow *KisFlow) GetFuncConfigByName(funcName string) *config.KisFuncConfig {
	if f, ok := flow.Funcs[funcName]; ok {
		return f.GetConfig()
	} else {
		log.Logger().ErrorF("GetFuncConfigByName(): Function %s not found", funcName)
		return nil
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

	//  数据data
	flow.data = make(common.KisDataMap)
	// 初始化临时数据
	flow.metaData = make(map[string]interface{})
	return flow
}

// Next 当前Flow执行到的Function进入下一层Function所携带的Action动作
func (flow *KisFlow) Next(acts ...kis.ActionFunc) error {

	// 加载Function FaaS 传递的 Action动作
	flow.action = kis.LoadActions(acts)

	return nil
}

// dealAction  处理Action，决定接下来Flow的流程走向
func (flow *KisFlow) dealAction(ctx context.Context, fn kis.Function) (kis.Function, error) {

	// DataReuse Action
	if flow.action.DataReuse {
		if err := flow.commitReuseData(ctx); err != nil {
			return nil, err
		}
	} else {
		if err := flow.commitCurData(ctx); err != nil {
			return nil, err
		}
	}

	// ForceEntryNext Action
	if flow.action.ForceEntryNext {
		if err := flow.commitVoidData(ctx); err != nil {
			return nil, err
		}
		flow.abort = false
	}
	// JumpFunc Action
	if flow.action.JumpFunc != "" {
		if _, ok := flow.Funcs[flow.action.JumpFunc]; !ok {
			//当前JumpFunc不在flow中
			return nil, errors.New(fmt.Sprintf("Flow Jump -> %s is not in Flow", flow.action.JumpFunc))
		}

		jumpFunction := flow.Funcs[flow.action.JumpFunc]
		// 更新上层Function
		flow.PrevFunctionId = jumpFunction.GetPrevId()
		fn = jumpFunction

		// 如果设置跳跃，强制跳跃
		flow.abort = false
	} else {

		// 更新上一层 FuncitonId 游标
		flow.PrevFunctionId = flow.ThisFunctionId
		fn = fn.Next()
	}

	// Abort Action 强制终止
	if flow.action.Abort {
		flow.abort = true
	}

	// 清空Action
	flow.action = kis.Action{}

	return fn, nil
}

func (flow *KisFlow) commitVoidData(ctx context.Context) error {
	if len(flow.buffer) != 0 {
		return nil
	}

	// 制作空数据
	batch := make(common.KisRowArr, 0)

	// 将本层计算的缓冲数据提交到本层结果数据中
	flow.data[flow.ThisFunctionId] = batch

	log.Logger().DebugFX(ctx, " ====> After commitVoidData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}

// commitReuseData
func (flow *KisFlow) commitReuseData(ctx context.Context) error {

	// 判断上层是否有结果数据, 如果没有则退出本次Flow Run循环
	if len(flow.data[flow.PrevFunctionId]) == 0 {
		flow.abort = true
		return nil
	}

	// 本层结果数据等于上层结果数据(复用上层结果数据到本层)
	flow.data[flow.ThisFunctionId] = flow.data[flow.PrevFunctionId]

	// 清空缓冲Buf (如果是ReuseData选项，那么提交的全部数据，都将不会携带到下一层)
	flow.buffer = flow.buffer[0:0]

	log.Logger().DebugFX(ctx, " ====> After commitReuseData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}
func (flow *KisFlow) GetCacheData(key string) interface{} {

	if data, found := flow.cache.Get(key); found {
		return data
	}

	return nil
}

func (flow *KisFlow) SetCacheData(key string, value interface{}, Exp time.Duration) {
	if Exp == common.DefaultExpiration {
		flow.cache.Set(key, value, cache.DefaultExpiration)
	} else {
		flow.cache.Set(key, value, Exp)
	}
}

// GetMetaData 得到当前Flow对象的临时数据
func (flow *KisFlow) GetMetaData(key string) interface{} {
	flow.mLock.RLock()
	defer flow.mLock.RUnlock()

	data, ok := flow.metaData[key]
	if !ok {
		return nil
	}

	return data
}

// SetMetaData 设置当前Flow对象的临时数据
func (flow *KisFlow) SetMetaData(key string, value interface{}) {
	flow.mLock.Lock()
	defer flow.mLock.Unlock()

	flow.metaData[key] = value
}

// Fork 得到Flow的一个副本(深拷贝)
func (flow *KisFlow) Fork(ctx context.Context) kis.Flow {

	config := flow.Conf

	// 通过之前的配置生成一个新的Flow
	newFlow := NewKisFlow(config)

	for _, fp := range flow.Conf.Flows {
		if _, ok := flow.funcParams[flow.Funcs[fp.FuncName].GetId()]; !ok {
			//当前function没有配置Params
			newFlow.Link(flow.Funcs[fp.FuncName].GetConfig(), nil)
		} else {
			//当前function有配置Params
			newFlow.Link(flow.Funcs[fp.FuncName].GetConfig(), fp.Params)
		}
	}

	log.Logger().DebugFX(ctx, "=====>Flow Fork, oldFlow.funcParams = %+v\n", flow.funcParams)
	log.Logger().DebugFX(ctx, "=====>Flow Fork, newFlow.funcParams = %+v\n", newFlow.GetFuncParamsAllFuncs())

	return newFlow
}

// GetFuncParamsAllFuncs 得到Flow中所有Function的FuncParams，取出全部Key-Value
func (flow *KisFlow) GetFuncParamsAllFuncs() map[string]config.FParam {
	flow.fplock.RLock()
	defer flow.fplock.RUnlock()

	return flow.funcParams
}
