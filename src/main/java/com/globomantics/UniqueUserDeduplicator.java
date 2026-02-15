package com.globomantics;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Drops duplicate userIds per adId using keyed state (with a 24h TTL). */
public class UniqueUserDeduplicator
    extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {

  private transient MapState<String, Boolean> seenUsers;

  @Override
  public void open(OpenContext openContext) throws Exception {
    MapStateDescriptor<String, Boolean> desc =
        new MapStateDescriptor<>("seenUsers", Types.STRING, Types.BOOLEAN);

    StateTtlConfig ttl =
        StateTtlConfig.newBuilder(Duration.ofDays(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .build();

    desc.enableTimeToLive(ttl);
    seenUsers = getRuntimeContext().getMapState(desc);
  }

  @Override
  public void processElement(
      ClickEvent e,
      Context ctx,
      Collector<ClickEvent> out) throws Exception {

    final String user = e.getUserId();

    if (!seenUsers.contains(user)) {
      seenUsers.put(user, true);
      out.collect(e);
    }
  }
}


/* 
package com.globomantics;

import java.time.Duration;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
*/
/** Drops duplicate userIds per adId using keyed state (with a 24h TTL). */
/* 
public class UniqueUserDeduplicator
    extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {

  private transient MapState<String, Boolean> seenUsers;

  @Override
  public void open(Configuration parameters) throws Exception {

    MapStateDescriptor<String, Boolean> desc =
        new MapStateDescriptor<>("seenUsers", Types.STRING, Types.BOOLEAN);

    // TTL = 1 day using java.time.Duration (Flink 2.0 style)
    StateTtlConfig ttl =
        StateTtlConfig.newBuilder(Duration.ofDays(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .build();

    desc.enableTimeToLive(ttl);

    seenUsers = getRuntimeContext().getMapState(desc);
  }

  @Override
  public void processElement(
      ClickEvent e,
      Context ctx,
      Collector<ClickEvent> out) throws Exception {

    final String user = e.getUserId();

    if (!seenUsers.contains(user)) {
      seenUsers.put(user, true);
      out.collect(e);
    }
  }
}

*/
/*
package com.globomantics;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
//import org.apache.flink.api.common.time.Time;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
*/
/** Drops duplicate userIds per adId using keyed state (with a 24h TTL). */
/* 
public class UniqueUserDeduplicator extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {
  private transient MapState<String, Boolean> seenUsers;

  @Override
  //public void open(Configuration parameters) {
  public void open(Configuration parameters) throws Exception {
    MapStateDescriptor<String, Boolean> desc =
        new MapStateDescriptor<>("seenUsers", Types.STRING, Types.BOOLEAN);

    //StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(1))
    StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofDays(1))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .build();
    desc.enableTimeToLive(ttl);

    seenUsers = getRuntimeContext().getMapState(desc);
  }

  @Override
  public void processElement(ClickEvent e, Context ctx, Collector<ClickEvent> out) throws Exception {
    final String user = e.getUserId();
    if (!seenUsers.contains(user)) {
      seenUsers.put(user, true);
      out.collect(e);
    }
  }
}
*/