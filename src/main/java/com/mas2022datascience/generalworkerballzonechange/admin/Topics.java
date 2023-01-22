package com.mas2022datascience.generalworkerballzonechange.admin;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class Topics {

  @Value(value = "${topic.general-01.name}")
  private String topicNamePlayerBallCompact;
  @Value(value = "${topic.general-01.partitions}")
  private Integer topicPartitionsPlayerBallCompact;
  @Value(value = "${topic.general-01.replication-factor}")
  private Integer topicReplicationFactorPlayerBallCompact;

  // creates or alters the topic
  @Bean
  public NewTopic general01() {
    return TopicBuilder.name(topicNamePlayerBallCompact)
        .partitions(topicPartitionsPlayerBallCompact)
        .replicas(topicReplicationFactorPlayerBallCompact)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-02.name}")
  private String topicNameBallPossessionChange;
  @Value(value = "${topic.general-02.partitions}")
  private Integer topicPartitionsBallPossessionChange;
  @Value(value = "${topic.general-02.replication-factor}")
  private Integer topicReplicationFactorBallPossessionChange;

  // creates or alters the topic
  @Bean
  public NewTopic general02() {
    return TopicBuilder.name(topicNameBallPossessionChange)
        .partitions(topicPartitionsBallPossessionChange)
        .replicas(topicReplicationFactorBallPossessionChange)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-match-team.name}")
  private String topicNameGeneralMatchTeam;
  @Value(value = "${topic.general-match-team.partitions}")
  private Integer topicPartitionsGeneralMatchTeam;
  @Value(value = "${topic.general-match-team.replication-factor}")
  private Integer topicReplicationFactorGeneralMatchTeam;

  // creates or alters the topic
  @Bean
  public NewTopic generalMatchTeam() {
    return TopicBuilder.name(topicNameGeneralMatchTeam)
        .partitions(topicPartitionsGeneralMatchTeam)
        .replicas(topicReplicationFactorGeneralMatchTeam)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }
}