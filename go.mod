module github.com/deviceinsight/kafkactl

require (
	github.com/DataDog/zstd v1.4.4 // indirect
	github.com/Shopify/sarama v1.26.0
	github.com/landoop/schema-registry v0.0.0-20190327143759-50a5701c1891
	github.com/linkedin/goavro v0.0.0-20180427113916-2f3e1dff9761
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/russross/blackfriday v2.0.0+incompatible // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.2
	go.uber.org/atomic v1.5.1 // indirect
	go.uber.org/ratelimit v0.1.0
	golang.org/x/sys v0.0.0-20200124204421-9fbb57f87de9 // indirect
	gopkg.in/ini.v1 v1.51.1 // indirect
	gopkg.in/yaml.v2 v2.2.8
)

replace github.com/russross/blackfriday => github.com/russross/blackfriday v1.5.2

replace github.com/Shopify/sarama => ../../Shopify/sarama

go 1.13
