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

/**
 * Stateless bolt that filters a specific tortoise and transforms the schema.
 * Input schema: (id, top, tour, cellule, total, maxcel) from JSON
 * Output schema: (id, top, nom, nbCellsParcourus, total, maxcel)
 * 
 * @author TP GGMD
 */
public class MyTortoiseBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
	private OutputCollector collector;
	private int targetTortoiseId;
	private String tortoiseName;
	
	/**
	 * Constructor that specifies which tortoise to filter and its name.
	 * @param targetTortoiseId The ID of the tortoise to filter (e.g., 3)
	 * @param tortoiseName The friendly name of the tortoise (e.g., "Caroline")
	 */
	public MyTortoiseBolt(int targetTortoiseId, String tortoiseName) {
		this.targetTortoiseId = targetTortoiseId;
		this.tortoiseName = tortoiseName;
	}
	
	/**
	 * Process each incoming tuple.
	 * Filters tuples to only those matching the target tortoise ID,
	 * transforms the schema, and emits the result.
	 */
	public void execute(Tuple t) {
		try {
			// Get the incoming JSON string
			String jsonString = t.getValueByField("json").toString();
			
			// Parse the JSON manually by extracting values
			int id = extractIntFromJson(jsonString, "id");
			int top = extractIntFromJson(jsonString, "top");
			int tour = extractIntFromJson(jsonString, "tour");
			int cellule = extractIntFromJson(jsonString, "cellule");
			int total = extractIntFromJson(jsonString, "total");
			int maxcel = extractIntFromJson(jsonString, "maxcel");
			
			// Filter: only process if this is our target tortoise
			if (id == targetTortoiseId) {
				// Calculate nbCellsParcourus: total distance = (tour * maxcel) + cellule
				int nbCellsParcourus = (tour * maxcel) + cellule;
				
				logger.info("Processing tortoise " + tortoiseName + " (id=" + id + 
						   ", top=" + top + ", nbCellsParcourus=" + nbCellsParcourus + ")");
				
				// Emit with the new schema
				collector.emit(t, new Values(id, top, tortoiseName, nbCellsParcourus, total, maxcel));
				collector.ack(t);
			} else {
				// Discard tuples from other tortoises
				collector.ack(t);
			}
			
		} catch (Exception e) {
			logger.severe("Error processing tuple: " + e.getMessage());
			e.printStackTrace();
			collector.fail(t);
		}
	}
	
	/**
	 * Helper method to extract an integer value from a JSON string.
	 * @param json The JSON string
	 * @param key The key to extract
	 * @return The integer value associated with the key
	 */
	private int extractIntFromJson(String json, String key) {
		String pattern = "\"" + key + "\"";
		int startIndex = json.indexOf(pattern) + pattern.length();
		int endIndex = json.indexOf(",", startIndex);
		if (endIndex == -1) {
			endIndex = json.indexOf("}", startIndex);
		}
		String valueStr = json.substring(startIndex, endIndex).replaceAll("[^0-9-]", "");
		return Integer.parseInt(valueStr);
	}
	
	/**
	 * Declare the output fields for this bolt.
	 * Output schema: (id, top, nom, nbCellsParcourus, total, maxcel)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
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
		logger.info("MyTortoiseBolt is being cleaned up.");
	}
	
	/**
	 * Prepare the bolt with the output collector.
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger.info("MyTortoiseBolt prepared for tortoise " + tortoiseName + " (id=" + targetTortoiseId + ")");
	}
}
