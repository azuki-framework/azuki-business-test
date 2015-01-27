/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.azkfw.business.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.azkfw.test.AbstractPersistenceTestCase;

/**
 * このクラスは、データベース機能をサポートしたタスククラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2015/01/27
 * @author Kawakicchi
 */
public abstract class AbstractDatabaseTestCase extends AbstractPersistenceTestCase {

	private static ConnectionFactory factory;
	private Connection connection;

	@Override
	public void setUp() {
		super.setUp();

		if (null == factory) {
			try {
				factory = ConnectionFactory.getInstance(getDatasourceProperties());
			} catch (Exception ex) {
				fail("Database error.");
			}
		}
	}

	@Override
	public void tearDown() {
		if (null != connection) {
			try {
				connection.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			connection = null;
		}

		super.tearDown();
	}

	protected final Connection getConnection() {
		try {
			if (null == connection) {
				connection = factory.getConnection();
				connection.setAutoCommit(false);
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
			fail("Database error.");
		}
		return connection;
	}

	protected Properties getDatasourceProperties() {
		Properties p = new Properties();
		p.setProperty("driverClassName", "");
		p.setProperty("url", "");
		p.setProperty("username", "");
		p.setProperty("password", "");

		p.setProperty("initialSize", "2");
		p.setProperty("maxActive", "5");
		p.setProperty("maxIdle", "2");
		p.setProperty("maxWait", "5000");
		p.setProperty("validationQuery", "select 1");
		return p;
	}

	private static abstract class ConnectionFactory {

		protected ConnectionFactory() {
		}

		public static ConnectionFactory getInstance(final Properties p) {
			return new DBCPConnectionFactory(p);
		}

		public abstract Connection getConnection() throws SQLException;

	}

	private static class DBCPConnectionFactory extends ConnectionFactory {

		private DataSource ds;

		protected DBCPConnectionFactory(final Properties p) {
			try {
				this.ds = BasicDataSourceFactory.createDataSource(p);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Connection getConnection() throws SQLException {
			return ds.getConnection();
		}
	}
}
