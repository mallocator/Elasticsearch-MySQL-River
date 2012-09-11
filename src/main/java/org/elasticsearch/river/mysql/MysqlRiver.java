package org.elasticsearch.river.mysql;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

/**
 *
 */
public class MysqlRiver extends AbstractRiverComponent implements River {
	private final Client	esClient;
	private final String	index;
	private final String	type;
	private volatile Thread	thread;
	private boolean			stopThread;

	private final String	url;
	private final String	username;
	private final String	password;
	private final String	query;
	private final String	uniqueIdField;
	private final boolean	deleteOldEntries;
	private final long		interval;

	@Inject
	public MysqlRiver(final RiverName riverName, final RiverSettings settings, final Client esClient) {
		super(riverName, settings);
		this.esClient = esClient;
		this.logger.info("Creating MySQL Stream River");

		this.index = readConfig("index", riverName.name());
		this.type = readConfig("type", "data");
		this.url = "jdbc:mysql://" + readConfig("hostname") + "/" + readConfig("database");
		this.username = readConfig("username");
		this.password = readConfig("password");
		this.query = readConfig("query");
		this.uniqueIdField = readConfig("uniqueIdField", null);
		this.deleteOldEntries = Boolean.parseBoolean(readConfig("deleteOldEntries", "true"));
		this.interval = Long.parseLong(readConfig("interval", "600000"));
	}

	private String readConfig(final String config) {
		final String result = readConfig(config, null);
		if (result == null) {
			this.logger.error("Unable to read required config {}. Aborting!", config);
			throw new InvalidParameterException("Unable to read required config " + config);
		}
		return result;
	}

	@SuppressWarnings({ "unchecked" })
	private String readConfig(final String config, final String defaultValue) {
		if (this.settings.settings().containsKey("mysql")) {
			Map<String, Object> mysqlSettings = (Map<String, Object>) this.settings.settings().get("mysql");
			return XContentMapValues.nodeStringValue(mysqlSettings.get(config), defaultValue);
		}
		return defaultValue;
	}

	public void start() {
		this.logger.info("starting mysql stream");
		try {
			this.esClient.admin()
				.indices()
				.prepareCreate(this.index)
				.addMapping(this.type, "{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true}}}")
				.execute()
				.actionGet();
			this.logger.info("Created Index {} with _timestamp mapping for {}", this.index, this.type);
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				this.logger.debug("Not creating Index {} as it already exists", this.index);
			}
			else if (ExceptionsHelper.unwrapCause(e) instanceof ElasticSearchException) {
				this.logger.debug("Mapping {}.{} already exists and will not be created", this.index, this.type);
			}
			else {
				this.logger.warn("failed to create index [{}], disabling river...", e, this.index);
				return;
			}
		}

		try {
			this.esClient.admin()
				.indices()
				.preparePutMapping(this.index)
				.setType(this.type)
				.setSource("{\"" + this.type + "\":{\"_timestamp\":{\"enabled\":true}}}")
				.setIgnoreConflicts(true)
				.execute()
				.actionGet();
		} catch (ElasticSearchException e) {
			this.logger.debug("Mapping already exists for index {} and type {}", this.index, this.type);
		}

		if (this.thread == null) {
			this.thread = EsExecutors.daemonThreadFactory(this.settings.globalSettings(), "mysql_slurper").newThread(new Parser());
			this.thread.start();
		}
	}

	public void close() {
		this.logger.info("Closing MySQL river");
		this.stopThread = true;
		this.thread = null;
	}

	/**
	 * Parser that asynchronously writes the data from MySQL into ElasticSearch.
	 * 
	 * @author Mallox
	 */
	private class Parser extends Thread {
		private Parser() {}

		@Override
		public void run() {
			MysqlRiver.this.logger.info("Mysql Import Thread has started");
			long lastRun = 0;
			while (!MysqlRiver.this.stopThread) {
				if (lastRun + MysqlRiver.this.interval < System.currentTimeMillis()) {
					lastRun = System.currentTimeMillis();
					parse();
					if (MysqlRiver.this.interval <= 0) {
						break;
					}
					if (!MysqlRiver.this.stopThread) {
						MysqlRiver.this.logger.info("Mysql Import Thread is waiting for {} Seconds until the next run",
							MysqlRiver.this.interval / 1000);
					}
				}
				try {
					sleep(1000);
				} catch (InterruptedException e) {}
			}
			MysqlRiver.this.logger.info("Mysql Import Thread has finished");
		}

		private void parse() throws ElasticSearchException {
			Connection con = null;
			Statement st = null;
			ResultSet rs = null;

			try {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(MysqlRiver.this.url, MysqlRiver.this.username, MysqlRiver.this.password);
				st = con.createStatement();
				rs = st.executeQuery(MysqlRiver.this.query);
				final ResultSetMetaData md = rs.getMetaData();

				rs.last();
				final int size = rs.getRow();
				String timestamp = String.valueOf((int) (System.currentTimeMillis() / 1000));
				int progress = 0;
				MysqlRiver.this.logger.info("Got {} results from MySQL database", size);

				if (size == 0) {
					MysqlRiver.this.logger.warn("Got 0 results from database. Aborting before we do some damage and remove still valid entries.");
					return;
				}
				rs.beforeFirst();
				while (rs.next() && !MysqlRiver.this.stopThread) {
					final HashMap<String, Object> rowMap = new HashMap<String, Object>();
					for (int i = 1; i <= md.getColumnCount(); i++) {
						rowMap.put(md.getColumnName(i), rs.getString(i));
					}
					IndexRequestBuilder builder = MysqlRiver.this.esClient.prepareIndex(MysqlRiver.this.index, MysqlRiver.this.type);
					if (MysqlRiver.this.uniqueIdField != null) {
						builder.setId((String) rowMap.get(MysqlRiver.this.uniqueIdField));
					}
					builder.setOpType(OpType.INDEX)
						.setReplicationType(ReplicationType.ASYNC)
						.setOperationThreaded(true)
						.setTimestamp(timestamp)
						.setSource(rowMap)
						.execute()
						.actionGet();
					if (++progress % 100 == 0) {
						MysqlRiver.this.logger.debug("Processed {} entries ({} percent done)",
							progress,
							Math.round((float) progress / (float) size * 100f));
					}
				}
				MysqlRiver.this.logger.info("Imported {} entries into ElasticSeach from MySQL!", size);
				if (MysqlRiver.this.deleteOldEntries) {
					MysqlRiver.this.logger.info("Removing old MySQL entries from ElasticSearch!");
					MysqlRiver.this.esClient.prepareDeleteByQuery(MysqlRiver.this.index)
						.setTypes(MysqlRiver.this.type)
						.setQuery(QueryBuilders.rangeQuery("_timestamp").lt(timestamp))
						.execute()
						.actionGet();
					MysqlRiver.this.logger.info("Old MySQL entries have been removed from ElasticSearch!");
				}
				else {
					MysqlRiver.this.logger.info("Not removing old MySQL entries from ElasticSearch");
				}
				MysqlRiver.this.logger.info("MySQL river has been completed");
				return;
			} catch (SQLException ex) {
				MysqlRiver.this.logger.error("Error trying to read data frm MySQL database!", ex);
			} catch (ClassNotFoundException ex) {
				MysqlRiver.this.logger.error("Error trying to load MySQL driver!", ex);
			} finally {
				try {
					if (rs != null) {
						rs.close();
					}
					if (st != null) {
						st.close();
					}
					if (con != null) {
						con.close();
					}

				} catch (SQLException ex) {
					MysqlRiver.this.logger.warn("Error closing MySQL connection properly.", ex);
				}
			}
		}
	}
}
