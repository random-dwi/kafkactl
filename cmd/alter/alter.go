package alter

import (
	"github.com/spf13/cobra"
)

var CmdAlter = &cobra.Command{
	Use:     "alter",
	Aliases: []string{"edit"},
	Short:   "alter topics, partitions",
}

func init() {
	CmdAlter.AddCommand(cmdAlterTopic)
	CmdAlter.AddCommand(cmdAlterPartition)
}
