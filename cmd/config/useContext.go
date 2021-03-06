package config

import (
	"github.com/deviceinsight/kafkactl/output"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
)

var cmdUseContext = &cobra.Command{
	Use:     "use-context",
	Aliases: []string{"useContext"},
	Short:   "switch active context",
	Long:    `command to switch active context`,
	Args:    cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		context := strings.Join(args, " ")

		contexts := viper.GetStringMap("contexts")

		// check if it is an existing context
		if _, ok := contexts[context]; !ok {
			output.Failf("not a valid context:", context)
		}

		viper.Set("current-context", context)

		if err := viper.WriteConfig(); err != nil {
			output.Failf("Unable to write config:", err)
		}
	},
}

func init() {

}
