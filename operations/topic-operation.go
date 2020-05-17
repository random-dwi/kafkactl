package operations

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/deviceinsight/kafkactl/output"
	"github.com/deviceinsight/kafkactl/util"
	"sort"
	"strconv"
	"strings"
	"time"
)

type topic struct {
	Name       string
	Partitions []partition `json:",omitempty" yaml:",omitempty"`
	Configs    []config    `json:",omitempty" yaml:",omitempty"`
}

type partition struct {
	Id           int32
	OldestOffset int64   `json:"oldestOffset" yaml:"oldestOffset"`
	NewestOffset int64   `json:"newestOffset" yaml:"newestOffset"`
	Leader       string  `json:",omitempty" yaml:",omitempty"`
	Replicas     []int32 `json:",omitempty" yaml:",omitempty,flow"`
	ISRs         []int32 `json:"inSyncReplicas,omitempty" yaml:"inSyncReplicas,omitempty,flow"`
}

type requestedTopicFields struct {
	partitionId       bool
	partitionOffset   bool
	partitionLeader   bool
	partitionReplicas bool
	partitionISRs     bool
	config            bool
}

var allFields = requestedTopicFields{partitionId: true, partitionOffset: true, partitionLeader: true, partitionReplicas: true, partitionISRs: true, config: true}

type config struct {
	Name  string
	Value string
}

type GetTopicsFlags struct {
	OutputFormat string
}

type CreateTopicFlags struct {
	Partitions        int32
	ReplicationFactor int16
	ValidateOnly      bool
	Configs           []string
}

type AlterTopicFlags struct {
	Partitions        int32
	ReplicationFactor int16
	ValidateOnly      bool
	Configs           []string
}

type DescribeTopicFlags struct {
	PrintConfigs bool
	OutputFormat string
}

type TopicOperation struct {
}

func (operation *TopicOperation) CreateTopics(topics []string, flags CreateTopicFlags) {

	context := CreateClientContext()

	var (
		err   error
		admin sarama.ClusterAdmin
	)

	if admin, err = CreateClusterAdmin(&context); err != nil {
		output.Failf("failed to create cluster admin: %v", err)
	}

	topicDetails := sarama.TopicDetail{
		NumPartitions:     flags.Partitions,
		ReplicationFactor: flags.ReplicationFactor,
		ConfigEntries:     map[string]*string{},
	}

	for _, config := range flags.Configs {
		configParts := strings.Split(config, "=")
		topicDetails.ConfigEntries[configParts[0]] = &configParts[1]
	}

	for _, topic := range topics {
		if err = admin.CreateTopic(topic, &topicDetails, flags.ValidateOnly); err != nil {
			output.Failf("failed to create topic: %v", err)
		} else {
			output.Infof("topic created: %s", topic)
		}
	}
}

func (operation *TopicOperation) DeleteTopics(topics []string) {

	context := CreateClientContext()

	var (
		err   error
		admin sarama.ClusterAdmin
	)

	if admin, err = CreateClusterAdmin(&context); err != nil {
		output.Failf("failed to create cluster admin: %v", err)
	}

	for _, topic := range topics {
		if err = admin.DeleteTopic(topic); err != nil {
			output.Failf("failed to delete topic: %v", err)
		} else {
			output.Infof("topic deleted: %s", topic)
		}
	}
}

func (operation *TopicOperation) DescribeTopic(topic string, flags DescribeTopicFlags) {

	context := CreateClientContext()

	var (
		client sarama.Client
		admin  sarama.ClusterAdmin
		err    error
		exists bool
	)

	if client, err = CreateClient(&context); err != nil {
		output.Failf("failed to create client err=%v", err)
	}

	if exists, err = TopicExists(&client, topic); err != nil {
		output.Failf("failed to read topics err=%v", err)
	}

	if !exists {
		output.Failf("topic '%s' does not exist", topic)
	}

	if admin, err = CreateClusterAdmin(&context); err != nil {
		output.Failf("failed to create cluster admin: %v", err)
	}

	var t, _ = readTopic(&client, &admin, topic, allFields)

	if flags.PrintConfigs {
		if flags.OutputFormat == "json" || flags.OutputFormat == "yaml" {
			t.Configs = nil
		} else {
			configTableWriter := output.CreateTableWriter()
			configTableWriter.WriteHeader("CONFIG", "VALUE")

			for _, c := range t.Configs {
				configTableWriter.Write(c.Name, c.Value)
			}

			configTableWriter.Flush()
			output.PrintStrings("")
		}
	}

	partitionTableWriter := output.CreateTableWriter()

	if flags.OutputFormat == "" || flags.OutputFormat == "wide" {
		partitionTableWriter.WriteHeader("PARTITION", "OLDEST_OFFSET", "NEWEST_OFFSET", "LEADER", "REPLICAS", "IN_SYNC_REPLICAS")
	} else if flags.OutputFormat != "json" && flags.OutputFormat != "yaml" {
		output.Failf("unknown outputFormat: %s", flags.OutputFormat)
	}

	if flags.OutputFormat == "json" || flags.OutputFormat == "yaml" {
		output.PrintObject(t, flags.OutputFormat)
	} else if flags.OutputFormat == "wide" || flags.OutputFormat == "" {
		for _, p := range t.Partitions {
			replicas := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.Replicas)), ","), "[]")
			inSyncReplicas := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.ISRs)), ","), "[]")
			partitionTableWriter.Write(strconv.Itoa(int(p.Id)), strconv.Itoa(int(p.OldestOffset)), strconv.Itoa(int(p.NewestOffset)), p.Leader, replicas, inSyncReplicas)
		}
	}

	if flags.OutputFormat == "" || flags.OutputFormat == "wide" {
		partitionTableWriter.Flush()
	}
}

func (operation *TopicOperation) AlterTopic(topic string, flags AlterTopicFlags) {

	context := CreateClientContext()

	var (
		client sarama.Client
		admin  sarama.ClusterAdmin
		err    error
		exists bool
	)

	if client, err = CreateClient(&context); err != nil {
		output.Failf("failed to create client err=%v", err)
	}

	if exists, err = TopicExists(&client, topic); err != nil {
		output.Failf("failed to read topics err=%v", err)
	}

	if !exists {
		output.Failf("topic '%s' does not exist", topic)
	}

	if admin, err = CreateClusterAdmin(&context); err != nil {
		output.Failf("failed to create cluster admin: %v", err)
	}

	var t, _ = readTopic(&client, &admin, topic, requestedTopicFields{partitionId: true, config: true})

	if flags.Partitions != 0 {
		if len(t.Partitions) > int(flags.Partitions) {
			output.Failf("Decreasing the number of partitions is not supported")
		}

		var emptyAssignment = make([][]int32, 0)

		err = admin.CreatePartitions(topic, flags.Partitions, emptyAssignment, flags.ValidateOnly)
		if err != nil {
			output.Failf("Could not create partitions for topic '%s': %v", topic, err)
		}
	}

	if flags.ReplicationFactor > 0 {

		var brokers = client.Brokers()

		if int(flags.ReplicationFactor) > len(brokers) {
			output.Failf("Replication factor for topic '%s' must not exceed the number of available brokers", topic)
		}

		t, _ = readTopic(&client, &admin, topic, requestedTopicFields{partitionId: true, partitionReplicas: true})

		brokerReplicaCount := make(map[int32]int)
		for _, broker := range brokers {
			brokerReplicaCount[broker.ID()] = 0
		}

		for _, partition := range t.Partitions {
			for _, brokerId := range partition.Replicas {
				brokerReplicaCount[brokerId] += 1
			}
		}

		var replicaAssignment = make([][]int32, 0, int16(len(t.Partitions)))

		for _, partition := range t.Partitions {

			var replicas = getTargetReplicas(partition.Replicas, brokerReplicaCount, flags.ReplicationFactor)
			replicaAssignment = append(replicaAssignment, replicas)
		}

		output.Infof("assign %v", replicaAssignment)

		go func() {
			err = admin.AlterPartitionReassignments(topic, replicaAssignment)
			if err != nil {
				output.Failf("Could not change replicas for topic '%s': %v", topic, err)
			}
		}()


		partitions := make([]int32, 0)

		for _, partition := range t.Partitions {
			partitions = append(partitions, partition.Id)
		}

		for true {
			reassignments, err := admin.ListPartitionReassignments(topic, partitions)
			if err != nil {
				output.Warnf("Could not get replica assignment status for topic '%s': %v", topic, err)
			} else {
				for partitionId, status := range reassignments[t.Name] {
					output.Infof("partition %d replicas %v adding %v removing %v", partitionId, (*status).Replicas, (*status).AddingReplicas, (*status).RemovingReplicas)
				}
			}

			time.Sleep(1000 * time.Millisecond)
		}
	}

	if len(flags.Configs) == 0 {
		operation.DescribeTopic(topic, DescribeTopicFlags{})
		return
	}

	mergedConfigEntries := make(map[string]*string)

	for i, config := range t.Configs {
		mergedConfigEntries[config.Name] = &(t.Configs[i].Value)
	}

	for _, config := range flags.Configs {
		configParts := strings.Split(config, "=")

		if len(configParts) == 2 {
			if len(configParts[1]) == 0 {
				delete(mergedConfigEntries, configParts[0])
			} else {
				mergedConfigEntries[configParts[0]] = &configParts[1]
			}
		}
	}

	if err = admin.AlterConfig(sarama.TopicResource, topic, mergedConfigEntries, flags.ValidateOnly); err != nil {
		output.Failf("Could not alter topic config '%s': %v", topic, err)
	}

	operation.DescribeTopic(topic, DescribeTopicFlags{})
}

func getTargetReplicas(currentReplicas []int32, brokerReplicaCount map[int32]int, targetReplicationFactor int16) []int32 {

	replicas := currentReplicas

	for len(replicas) > int(targetReplicationFactor) {

		sort.Slice(replicas, func(i, j int) bool {
			brokerI := replicas[i]
			brokerJ := replicas[j]
			return brokerReplicaCount[brokerI] < brokerReplicaCount[brokerJ] || (brokerReplicaCount[brokerI] == brokerReplicaCount[brokerJ] && brokerI < brokerJ)
		})

		lastReplica := replicas[len(replicas)-1]
		replicas = replicas[:len(replicas)-1]
		brokerReplicaCount[lastReplica] -= 1
	}

	var unusedBrokerIds []int32

	if len(replicas) < int(targetReplicationFactor) {
		for brokerId := range brokerReplicaCount {
			if !util.ContainsInt32(replicas, brokerId) {
				unusedBrokerIds = append(unusedBrokerIds, brokerId)
			}
		}
		if len(unusedBrokerIds) < (int(targetReplicationFactor) - len(replicas)) {
			output.Failf("not enough brokers")
		}
	}

	for len(replicas) < int(targetReplicationFactor) {

		sort.Slice(unusedBrokerIds, func(i, j int) bool {
			brokerI := unusedBrokerIds[i]
			brokerJ := unusedBrokerIds[j]
			return brokerReplicaCount[brokerI] < brokerReplicaCount[brokerJ] || (brokerReplicaCount[brokerI] == brokerReplicaCount[brokerJ] && brokerI < brokerJ)
		})


		replicas = append(replicas, unusedBrokerIds[0])
		brokerReplicaCount[unusedBrokerIds[0]] += 1
		unusedBrokerIds = unusedBrokerIds[1:]
	}

	return replicas
}

func (operation *TopicOperation) GetTopics(flags GetTopicsFlags) {

	context := CreateClientContext()

	var (
		err    error
		client sarama.Client
		admin  sarama.ClusterAdmin
		topics []string
	)

	if admin, err = CreateClusterAdmin(&context); err != nil {
		output.Failf("failed to create cluster admin: %v", err)
	}

	if client, err = CreateClient(&context); err != nil {
		output.Failf("failed to create client err=%v", err)
	}

	if topics, err = client.Topics(); err != nil {
		output.Failf("failed to read topics err=%v", err)
	}

	tableWriter := output.CreateTableWriter()
	var requestedFields requestedTopicFields

	if flags.OutputFormat == "" {
		requestedFields = requestedTopicFields{partitionId: true}
		tableWriter.WriteHeader("TOPIC", "PARTITIONS")
	} else if flags.OutputFormat == "compact" {
		tableWriter.Initialize()
	} else if flags.OutputFormat == "wide" {
		requestedFields = requestedTopicFields{partitionId: true, config: true}
		tableWriter.WriteHeader("TOPIC", "PARTITIONS", "CONFIGS")
	} else if flags.OutputFormat == "json" {
		requestedFields = allFields
	} else if flags.OutputFormat == "yaml" {
		requestedFields = allFields
	} else {
		output.Failf("unknown outputFormat: %s", flags.OutputFormat)
	}

	topicChannel := make(chan topic)

	// read topics in parallel
	for _, topic := range topics {
		go func(topic string) {
			t, err := readTopic(&client, &admin, topic, requestedFields)
			if err != nil {
				output.Failf("unable to read topic %s: %v", topic, err)
			}
			topicChannel <- t
		}(topic)
	}

	topicList := make([]topic, 0, len(topics))
	for range topics {
		topicList = append(topicList, <-topicChannel)
	}

	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].Name < topicList[j].Name
	})

	if flags.OutputFormat == "json" || flags.OutputFormat == "yaml" {
		output.PrintObject(topicList, flags.OutputFormat)
	} else if flags.OutputFormat == "wide" {
		for _, t := range topicList {
			tableWriter.Write(t.Name, strconv.Itoa(len(t.Partitions)), getConfigString(t.Configs))
		}
	} else if flags.OutputFormat == "compact" {
		for _, t := range topicList {
			tableWriter.Write(t.Name)
		}
	} else {
		for _, t := range topicList {
			tableWriter.Write(t.Name, strconv.Itoa(len(t.Partitions)))
		}
	}

	if flags.OutputFormat == "wide" || flags.OutputFormat == "compact" || flags.OutputFormat == "" {
		tableWriter.Flush()
	}
}

func readTopic(client *sarama.Client, admin *sarama.ClusterAdmin, name string, requestedFields requestedTopicFields) (topic, error) {
	var (
		err           error
		ps            []int32
		led           *sarama.Broker
		configEntries []sarama.ConfigEntry
		top           = topic{Name: name}
	)

	if !requestedFields.partitionId {
		return top, nil
	}

	if ps, err = (*client).Partitions(name); err != nil {
		return top, err
	}

	partitionChannel := make(chan partition)

	// read partitions in parallel
	for _, p := range ps {

		go func(partitionId int32) {

			np := partition{Id: partitionId}

			if requestedFields.partitionOffset {
				if np.OldestOffset, err = (*client).GetOffset(name, partitionId, sarama.OffsetOldest); err != nil {
					output.Failf("unable to read oldest offset for topic %s partition %d", name, partitionId)
				}

				if np.NewestOffset, err = (*client).GetOffset(name, partitionId, sarama.OffsetNewest); err != nil {
					output.Failf("unable to read newest offset for topic %s partition %d", name, partitionId)
				}
			}

			if requestedFields.partitionLeader {
				if led, err = (*client).Leader(name, partitionId); err != nil {
					output.Failf("unable to read leader for topic %s partition %d", name, partitionId)
				}
				np.Leader = led.Addr()
			}

			if requestedFields.partitionReplicas {
				if np.Replicas, err = (*client).Replicas(name, partitionId); err != nil {
					output.Failf("unable to read replicas for topic %s partition %d", name, partitionId)
				}
				sort.Slice(np.Replicas, func(i, j int) bool { return np.Replicas[i] < np.Replicas[j] })
			}

			if requestedFields.partitionISRs {
				if np.ISRs, err = (*client).InSyncReplicas(name, partitionId); err != nil {
					output.Failf("unable to read inSyncReplicas for topic %s partition %d", name, partitionId)
				}
				sort.Slice(np.ISRs, func(i, j int) bool { return np.ISRs[i] < np.ISRs[j] })
			}

			partitionChannel <- np
		}(p)
	}

	for range ps {
		top.Partitions = append(top.Partitions, <-partitionChannel)
	}

	sort.Slice(top.Partitions, func(i, j int) bool {
		return top.Partitions[i].Id < top.Partitions[j].Id
	})

	if requestedFields.config {

		configResource := sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: name,
		}

		if configEntries, err = (*admin).DescribeConfig(configResource); err != nil {
			output.Failf("failed to describe config: %v", err)
		}

		for _, configEntry := range configEntries {

			if !configEntry.Default && configEntry.Source != sarama.SourceDefault {
				entry := config{Name: configEntry.Name, Value: configEntry.Value}
				top.Configs = append(top.Configs, entry)
			}
		}
	}

	return top, nil
}

func getConfigString(configs []config) string {

	var configStrings []string

	for _, config := range configs {
		configStrings = append(configStrings, fmt.Sprintf("%s=%s", config.Name, config.Value))
	}

	return strings.Trim(strings.Join(configStrings, ","), "[]")
}
