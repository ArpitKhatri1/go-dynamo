package config

type GlobalConfig struct {
	ReplicationFactorN uint32
	ReadAcknowledgeR   uint32
	WriteAcknowledgeW  uint32
}

var appConfig = &GlobalConfig{ // R+W > N
	ReplicationFactorN: 3,
	ReadAcknowledgeR:   2,
	WriteAcknowledgeW:  2,
}

func GetSystemConfig() *GlobalConfig {
	return appConfig
}
