package com.mas2022datascience.generalworkerballzonechange.processor;

import com.mas2022datascience.avro.v1.GeneralBallZoneChange;
import com.mas2022datascience.avro.v1.GeneralMatchPhase;
import com.mas2022datascience.avro.v1.GeneralMatchTeam;
import com.mas2022datascience.avro.v1.PlayerBall;
import com.mas2022datascience.util.Team;
import com.mas2022datascience.util.Zones;
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
import org.apache.kafka.streams.kstream.Produced;
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

    // match phase
    final Serde<GeneralMatchPhase> generalMatchPhaseSerde = new SpecificAvroSerde<>();
    generalMatchPhaseSerde.configure(serdeConfig, false); // `false` for record values
    KStream<String, GeneralMatchPhase> streamPhase = kStreamBuilder.stream(topicGeneralMatchPhase,
        Consumed.with(Serdes.String(), generalMatchPhaseSerde));
    KTable<String, GeneralMatchPhase> phases = streamPhase.toTable(Materialized.as("phasesStore"));

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
        .map((key, value) -> {
          return KeyValue.pair(key.split("-")[0], // remove the ball id
            GeneralBallZoneChange.newBuilder()
              .setTs(value.getTs())
              .setMatchId(value.getMatchId())
              .setHomeTeamId("HOME")
              .setAwayTeamId("AWAY")
              .setBallX(value.getX())
              .setBallY(value.getY())
              .setBallZ(value.getZ())
              // transformer sets the old zone in the field playerId expecting the home team to be left
              .setHomeZoneOld(Integer.parseInt(value.getPlayerId()))
              .setHomeZoneNew(value.getZone())
              // transformer sets the old zone in the field playerId expecting the away team to be left
              .setAwayZoneOld(Integer.parseInt(value.getPlayerId()))
              .setAwayZoneNew(value.getZone())
              .build());
        })
        .join(teams, (newValue, teamsValue) -> {
          newValue.setHomeTeamId(String.valueOf(teamsValue.getHomeTeamID()));
          newValue.setAwayTeamId(String.valueOf(teamsValue.getAwayTeamID()));
          return newValue;
        })
        .join(phases, (newValue, phasesValue) -> {
          // change zone depending on the left team (values are specified for the left team
          if (newValue.getHomeTeamId().equals(Team.getLeftTeamByTimestamp(newValue.getTs(), phasesValue))) {
           newValue.setAwayZoneOld(Zones.invertZoneNumbering(newValue.getHomeZoneOld()));
           newValue.setAwayZoneNew(Zones.invertZoneNumbering(newValue.getHomeZoneNew()));
          } else {
            newValue.setHomeZoneOld(Zones.invertZoneNumbering(newValue.getHomeZoneOld()));
            newValue.setHomeZoneNew(Zones.invertZoneNumbering(newValue.getHomeZoneNew()));
          }
          return newValue;
        })
        .to(topicOutBallPossessionChange, Produced.with(Serdes.String(),
            generalBallZoneChangeSerde));
        //.print(Printed.<String, GeneralBallZoneChange>toSysOut());

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

      if (oldPlayerBall.getIsBallInPlay().equals("Alive") && value.getIsBallInPlay()
          .equals("Alive")) {
        if (Zones.isInSameZone(oldPlayerBall, value)) {
          stateStore.put(key, value);
          return new KeyValue<>(key, null);
        } else {
          // new zone under the estimate of being team left
          value.setZone(Zones.getZoneLeft(value.getX(), value.getY()));
          // old zone under the estimate of being team left
          value.setPlayerId(
              String.valueOf(Zones.getZoneLeft(oldPlayerBall.getX(), oldPlayerBall.getY())));
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
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


