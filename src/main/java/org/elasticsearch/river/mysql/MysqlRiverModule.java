package org.elasticsearch.river.mysql;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class MysqlRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(MysqlRiver.class).asEagerSingleton();
	}
}
