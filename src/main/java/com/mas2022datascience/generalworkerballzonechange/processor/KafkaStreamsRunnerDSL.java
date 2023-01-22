package com.mas2022datascience.generalworkerballzonechange.processor;

import com.mas2022datascience.avro.v1.GeneralBallZoneChange;
import com.mas2022datascience.avro.v1.GeneralMatchTeam;
import com.mas2022datascience.avro.v1.PlayerBall;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;
  @Value(value = "${topic.general-01.name}") private String topicIn;
  @Value(value = "${topic.general-02.name}") private String topicOutBallPossessionChange;
  @Value(value = "${topic.general-match-team.name}") private String topicGeneralMatchTeam;
  @Value(value = "${topic.general-match-phase.name}") private String topicGeneralMatchPhase;
  @Bean
  public KStream<String, PlayerBall> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);

    final Serde<PlayerBall> playerBallSerde = new SpecificAvroSerde<>();
    playerBallSerde.configure(serdeConfig, false); // `false` for record values

    final Serde<GeneralBallZoneChange> generalBallZoneChangeSerde = new
        SpecificAvroSerde<>();
    generalBallZoneChangeSerde.configure(serdeConfig, false);//`false` for record values

    // match team
    final Serde<GeneralMatchTeam> generalMatchTeamSerde = new SpecificAvroSerde<>();
    generalMatchTeamSerde.configure(serdeConfig, false); // `false` for record values
    KStream<String, GeneralMatchTeam> streamTeam = kStreamBuilder.stream(topicGeneralMatchTeam,
        Consumed.with(Serdes.String(), generalMatchTeamSerde));
    KTable<String, GeneralMatchTeam> teams = streamTeam.toTable(Materialized.as("teamsStore"));

    KStream<String, PlayerBall> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), playerBallSerde));

    final StoreBuilder<KeyValueStore<String, PlayerBall>> myStateStore = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("MyPlayerBallStateStore"),
            Serdes.String(), playerBallSerde);
    kStreamBuilder.addStateStore(myStateStore);

    final MyPlayerBallHandler myStateHandler =
        new MyPlayerBallHandler(myStateStore.name());

    // invoke the transformer
    KStream<String, PlayerBall> transformedStream = stream
        .filter((key, value) -> value.getPlayerId().equals("0"))
        .transform(() -> myStateHandler, myStateStore.name());

    transformedStream
        .filter((key, values) -> values != null)//filter out the events where no zone change happened
        .mapValues((value) -> {
          return GeneralBallZoneChange.newBuilder()
              .setTs(value.getTs())
              .setMatchId(value.getMatchId())
              .setHomeTeamId("HOME")
              .setAwayTeamId("AWAY")
              .setBallX(value.getX())
              .setBallY(value.getY())
              .setBallZ(value.getZ())
              // transformer sets the old zone in the filed playerId
              .setHomeZoneOld(Integer.parseInt(value.getPlayerId()))
              .setHomeZoneNew(value.getZone())
              // transformer sets the old zone in the filed playerId
              .setAwayZoneOld(Integer.parseInt(value.getPlayerId()))
              .setAwayZoneNew(value.getZone())
              .build();
        })
        .leftJoin(teams, (newValue, teamsValue) -> {
            newValue.setHomeTeamId(String.valueOf(teamsValue.getHomeTeamID()));
            newValue.setAwayTeamId(String.valueOf(teamsValue.getAwayTeamID()));
          return newValue;
        })
//        .to(topicOutBallPossessionChange, Produced.with(Serdes.String(),
//            generalBallZoneChangeSerde));
        .print(Printed.<String, GeneralBallZoneChange>toSysOut());

    return stream;

  }

  private static final class MyPlayerBallHandler implements
      Transformer<String, PlayerBall, KeyValue<String, PlayerBall>> {
    final private String storeName;
    private KeyValueStore<String, PlayerBall> stateStore;

    public MyPlayerBallHandler(final String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      stateStore = processorContext.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, PlayerBall> transform(String key, PlayerBall value) {
      try {
        if (stateStore.get(key) == null) {
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
        }
      } catch (org.apache.kafka.common.errors.SerializationException ex) {
        // the first time the state store is empty, so we get a serialization exception
        stateStore.put(key, value);
        return new KeyValue<>(key, stateStore.get(key));
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }

      PlayerBall oldPlayerBall = stateStore.get(key);

      if (oldPlayerBall.getIsBallInPlay().equals("Alive") && value.getIsBallInPlay().equals("Alive")) {
        if (!oldPlayerBall.getZone().equals(value.getZone())) {
          value.setPlayerId(String.valueOf(oldPlayerBall.getZone()));
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
        } else {
          stateStore.put(key, value);
          return new KeyValue<>(key, null);
        }
      } else {
        stateStore.put(key, value);
        return new KeyValue<>(key, null);
      }
    }

    @Override
    public void close() { }
  }

}


