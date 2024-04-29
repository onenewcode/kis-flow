package kis

import (
	"context"
	"kis-flow/config"
)

type Connector interface {
	// Init 初始化Connector所关联的存储引擎链接等
	//  主要为当前Connector所关联的第三方存储引擎的初始化逻辑，如创建链接登操作，Init在Connector实例的生命周期只会被执行一次。
	Init() error
	// Call 调用Connector 外挂存储逻辑的读写操作
	// 主要为Connector的调度入口，相关存储的读写自定义逻辑是通过Call()方法来触发调度，具体的回调函数原型在Router模块定义。
	Call(ctx context.Context, flow Flow, args interface{}) error
	// GetId 获取Connector的ID
	GetId() string
	// GetName 获取Connector的名称
	GetName() string
	// GetConfig 获取Connector的配置信息
	GetConfig() *config.KisConnConfig
	GetMetaData(key string) interface{}
	// SetMetaData 设置当前Connector的临时数据
	SetMetaData(key string, value interface{})
}
