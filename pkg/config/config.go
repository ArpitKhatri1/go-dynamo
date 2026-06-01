package config

type GlobalConfig struct {
	ReplicationFactorN int
	ReadAcknowledgeR   int
	WriteAcknowledgeW  int
}

var appConfig = &GlobalConfig{ // R+W > N for quorum
	ReplicationFactorN: 3,
	ReadAcknowledgeR:   2,
	WriteAcknowledgeW:  2,
}

func GetSystemConfig() *GlobalConfig {
	return appConfig
}
