# KisFlow整体架构
##  为什么需要KisFlow
一些大型toB企业级的项目，需要大量的业务数据，多数的数据需要流式实时计算的能力，但是很多公司还不足以承担一个数仓类似，Flink + Hadoop/HBase 等等。 但是业务数据的实时计算需求依然存在，所以大多数的企业依然会让业务工程师来消化这些业务数据计算的工作。

而这样只能直接查询业务数据库，这样会间接影响的业务能力，或定时任务/脚本来做定时计算，这些都不是好的办法。本人亲身经历过一个大规模的系统，多达上千个需要计算的业务数据字段，而早期因为没有规划好，最后导致存在1000+的脚本在定时跑，最后导致了脚本之间对数据的影响，数据始终无法准确，导致业务数据经常性的报数据问题错误。
如下面一个场景：某个业务计算字段的值，正确为100，错误为99， 但是由于历史代码的臃肿，会有多个计算脚本对其值做修复补丁计算，会有各个脚本相互冲突，在一定的时间间隔内会存在数据值抖动，可能最终一个补丁修复正确，但是这种情况就会存在一定时间范围内业务数据不正确，最终却奇迹正确的情况，很让用户苦恼。
# V0.1-项目构建及基础模块定义
## 项目构建
```yaml
 kis-flow /
.
├── README.md
├── common/
├── example/
├── function/
├── conn/
├── config/
├── flow/
├──kis/
└──log/ 

```
- common/为 存放我们一些公用的基础常量和一些枚举参数，还有一些工具类的方法。
- flow/为存放KisFlow的核心代码。
- function/为存放KisFunction的核心代码。
- conn/为存放KisConnector的核心代码。
- config/ 存放flow、functioin、connector等策略配置信息模块。
- example/为我们针对KisFlow的一些测试案例和test单元测试案例等，能够及时验证我们的项目效果。
- kis/来存放所有模块的抽象层。
- 日志接口
## KisLogger
首先因为在之后会有很多调试日志要打印，我们先把日志模块集成了，日志模块KisFlow提供一个默认的标准输出Logger对象，再对我开放一个SetLogger() 方法来进行重新设置开发者自己的Logger模块。
### Logger抽象接口
将Logger的定义在kis-flow/log/目录下，创建kis_log.go文件：
>kis-flow/log/kis_log.go

```go
package log

import "context"

type KisLogger interface {
	// InfoFX 有上下文的Info级别日志接口, format字符串格式
	InfoFX(ctx context.Context, str string, v ...interface{})
	// ErrorFX 有上下文的Error级别日志接口, format字符串格式
	ErrorFX(ctx context.Context, str string, v ...interface{})
	// DebugFX 有上下文的Debug级别日志接口, format字符串格式
	DebugFX(ctx context.Context, str string, v ...interface{})

	// InfoF 无上下文的Info级别日志接口, format字符串格式
	InfoF(str string, v ...interface{})
	// ErrorF 无上下文的Error级别日志接口, format字符串格式
	ErrorF(str string, v ...interface{})
	// DebugF 无上下文的Debug级别日志接口, format字符串格式
	DebugF(str string, v ...interface{})
}

// kisLog 默认的KisLog 对象
var kisLog KisLogger

// SetLogger 设置KisLog对象, 可以是用户自定义的Logger对象
func SetLogger(newlog KisLogger) {
	kisLog = newlog
}

// Logger 获取到kisLog对象
func Logger() KisLogger {
	return kisLog
}
```
KisLogger提供了三个级别的日志，分别是Info、Error、Debug。且也分别提供了具备context参数与不具备context参数的两套日志接口。
提供一个全局对象kisLog，默认的KisLog 对象。以及方法SetLogger()和Logger()供开发可以设置自己的Logger对象以及获取到Logger对象。
### 默认的日志对象KisDefaultLogger
如果开发没有自定义的日志对象定义，那么KisFlow会提供一个默认的日志对象kisDefaultLogger，这个类实现了KisLogger的全部接口，且都是默认打印到标准输出的形式来打印日志，定义在kis-flow/log/目录下，创建kis_default_log.go文件。
>kis-flow/log/kis_default_log.go

```go
package log

import (
	"context"
	"fmt"
)

// kisDefaultLog 默认提供的日志对象
type kisDefaultLog struct{}

func (log *kisDefaultLog) InfoF(str string, v ...interface{}) {
	fmt.Printf(str, v...)
}

func (log *kisDefaultLog) ErrorF(str string, v ...interface{}) {
	fmt.Printf(str, v...)
}

func (log *kisDefaultLog) DebugF(str string, v ...interface{}) {
	fmt.Printf(str, v...)
}

func (log *kisDefaultLog) InfoFX(ctx context.Context, str string, v ...interface{}) {
	fmt.Println(ctx)
	fmt.Printf(str, v...)
}

func (log *kisDefaultLog) ErrorFX(ctx context.Context, str string, v ...interface{}) {
	fmt.Println(ctx)
	fmt.Printf(str, v...)
}

func (log *kisDefaultLog) DebugFX(ctx context.Context, str string, v ...interface{}) {
	fmt.Println(ctx)
	fmt.Printf(str, v...)
}

func init() {
	// 如果没有设置Logger, 则启动时使用默认的kisDefaultLog对象
	if Logger() == nil {
		SetLogger(&kisDefaultLog{})
	}
}
```
这里在init()初始化方法中，会判断目前是否已经有设置全局的Logger对象，如果没有，KisFlow会默认选择kisDefaultLog 作为全局Logger日志对象。

# config
在KisFlow中，我们定义了三种核心模块，分别是KisFunction, KisFlow, KisConnector ，所以KisConfig也分别需要针对三个模块进行定义。
## 配置文件定义
```yaml
kistype: func # func, flow, connector
fname: 测试KisFunction_S1 # 函数名称
fmode: Save # Save, Update, Delete
source: # 源数据源
  name: 被校验的测试数据源1-用户订单维度 # 数据源名称
  must: # 必须字段
    - userid
    - orderid
    
option: # 配置项
  cname: 测试KisConnector_1  # 连接器名称
  retry_times: 3 # 重试次数
  retry_duration: 500 # 重试间隔
  default_params: # 默认参数
    default1: default1_param
    default2: default2_param
```