package org.elasticsearch.plugin.river.mysql;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.mysql.MysqlRiverModule;

public class MysqlRiverPlugin extends AbstractPlugin {

	@Inject
	public MysqlRiverPlugin() {}

	public String name() {
		return "river-mysql";
	}

	public String description() {
		return "River MySQL Plugin";
	}

	public void onModule(final RiversModule module) {
		module.registerRiver("mysql", MysqlRiverModule.class);
	}
}
