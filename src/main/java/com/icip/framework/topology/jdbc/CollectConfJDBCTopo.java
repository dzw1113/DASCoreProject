/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.icip.framework.topology.jdbc;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.jdbc.DataSourceLoader;
import com.icip.das.core.kafka.KafkaProducerFactory;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.HandlerSQLDataFun;
import com.icip.framework.spout.MysqlSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

public class CollectConfJDBCTopo extends AbstractBaseTopo {

	private static final long serialVersionUID = 4231810093290495409L;

	public CollectConfJDBCTopo(String topologyName) {
		super(topologyName);
	}

	public static final String topologyName = "CollectConfJDBCTopo";

	// false本地 true线上
	private static boolean flag = false;

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {
		KafkaProducerFactory.getProducer();
		DataSourceLoader.DATASOURCE_INSTANCE.init();
		
		
		TridentTopology topology = new TridentTopology();
		MysqlSpout spout = new MysqlSpout();
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		stream.each(new Fields(DASConstants.SQL, DASConstants.TABLECONFIGBEAN),
				new HandlerSQLDataFun(), new Fields());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		new CollectConfJDBCTopo(topologyName).execute(flag);
	}

}
