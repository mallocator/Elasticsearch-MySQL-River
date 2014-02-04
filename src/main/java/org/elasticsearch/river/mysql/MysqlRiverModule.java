package org.elasticsearch.river.mysql;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * The Module that performs the actual binding of the MySQL module.
 * 
 * @author Ravi Gairola (ravig@motorola.com)
 */
public class MysqlRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(MysqlRiver.class).asEagerSingleton();
	}
}
