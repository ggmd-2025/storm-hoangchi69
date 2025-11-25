package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.stream.StreamEmiter;

/**
 * Exit bolt that transforms structured tuples into JSON format.
 * Input schema: (id, top, nom, nbCellsParcourus, total, maxcel)
 * Output schema: (json) - JSON string representation
 * 
 * @author TP GGMD
 */
public class Exit2Bolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger("Exit2BoltLogger");
	private OutputCollector collector;
	private int port;
	private StreamEmiter streamEmitter;
	
	/**
	 * Constructor that specifies the output port.
	 * @param port The port to emit the output stream to
	 */
	public Exit2Bolt(int port) {
		this.port = port;
		this.streamEmitter = new StreamEmiter(this.port);
	}
	
	/**
	 * Process each incoming tuple.
	 * Transforms the structured tuple into a JSON object and emits it.
	 */
	public void execute(Tuple t) {
		try {
			// Extract fields from the input tuple
			int id = t.getIntegerByField("id");
			int top = t.getIntegerByField("top");
			String nom = t.getStringByField("nom");
			int nbCellsParcourus = t.getIntegerByField("nbCellsParcourus");
			int total = t.getIntegerByField("total");
			int maxcel = t.getIntegerByField("maxcel");
			
			// Build JSON string manually
			StringBuilder jsonString = new StringBuilder();
			jsonString.append("{");
			jsonString.append("\"id\":").append(id).append(",");
			jsonString.append("\"top\":").append(top).append(",");
			jsonString.append("\"nom\":\"").append(nom).append("\",");
			jsonString.append("\"nbCellsParcourus\":").append(nbCellsParcourus).append(",");
			jsonString.append("\"total\":").append(total).append(",");
			jsonString.append("\"maxcel\":").append(maxcel);
			jsonString.append("}");
			
			String json = jsonString.toString();
			
			logger.info("Emitting JSON: " + json);
			
			// Send through the stream emitter
			this.streamEmitter.send(json);
			
			// Emit to the topology and acknowledge
			collector.emit(t, new Values(json));
			collector.ack(t);
			
		} catch (Exception e) {
			logger.severe("Error processing tuple: " + e.getMessage());
			e.printStackTrace();
			collector.fail(t);
		}
	}
	
	/**
	 * Declare the output fields for this bolt.
	 * Output schema: (json)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}
	
	/**
	 * Return component configuration (none specific in this case).
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	/**
	 * Cleanup method called when the bolt is being removed.
	 */
	public void cleanup() {
		logger.info("Exit2Bolt is being cleaned up.");
	}
	
	/**
	 * Prepare the bolt with the output collector.
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger.info("Exit2Bolt prepared on port " + port);
	}
}
