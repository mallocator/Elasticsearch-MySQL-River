package org.elasticsearch.plugin.river.mysql;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;

/**
 * Class for registering the MySQL river plugin.
 * 
 * @author Ravi Gairola (mallox@pyxzl.net)
 */
public class MysqlRiverPlugin extends AbstractPlugin {

	@Inject
	public MysqlRiverPlugin() {}

	@Override
	public String name() {
		return "river-mysql";
	}

	@Override
	public String description() {
		return "River MySQL Plugin";
	}
}
