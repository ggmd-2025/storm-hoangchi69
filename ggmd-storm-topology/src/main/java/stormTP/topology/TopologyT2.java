package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit2Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;

/**
 * Topology T2 for tortoise race filtering.
 * This topology filters a specific tortoise (ID=3, name="Caroline") from the input stream,
 * transforms its schema to include cell count and tortoise name,
 * and emits the result as JSON.
 * 
 * Input stream schema: (id, top, tour, cellule, total, maxcel)
 * Output stream schema: (json) containing {id, top, nom, nbCellsParcourus, total, maxcel}
 * 
 * @author TP GGMD
 */
public class TopologyT2 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = Integer.parseInt(args[0]);
		int portOUTPUT = Integer.parseInt(args[1]);
		
		// Configuration parameters for the tortoise filter
		int targetTortoiseId = 3;  // Filter tortoise with ID 3
		String tortoiseName = "Caroline";  // The name for this tortoise
    	
		// Create the input spout
		InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
		
		// Create the topology builder
		TopologyBuilder builder = new TopologyBuilder();
		
		// Add the spout to the topology
		builder.setSpout("masterStream", spout);
		
		// Add the tortoise filter bolt
		// It will filter for tortoise ID 3 and rename it to "Caroline"
		builder.setBolt("tortoiseMatcher", new MyTortoiseBolt(targetTortoiseId, tortoiseName), nbExecutors)
			.shuffleGrouping("masterStream");
		
		// Add the exit bolt that converts to JSON
		builder.setBolt("exit", new Exit2Bolt(portOUTPUT), nbExecutors)
			.shuffleGrouping("tortoiseMatcher");
		
		// Create configuration
		Config config = new Config();
		
		// Submit the topology to STORM
		StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
	}
}
