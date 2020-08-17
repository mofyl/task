package task

import "github.com/beinan/fastid"


func GetUUID () int64{
	// 40bit的时间戳 + 16bit的机器ID + 7bit 的seq number
	// 机器ID 这里取的是本机IP的后16bit
	return fastid.CommonConfig.GenInt64ID()
}
