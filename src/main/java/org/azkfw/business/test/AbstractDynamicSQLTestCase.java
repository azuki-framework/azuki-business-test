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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.azkfw.business.dao.DataAccessServiceException;
import org.azkfw.business.dao.DynamicSQLAccessObject;
import org.azkfw.dsql.DynamicSQL;
import org.azkfw.dsql.DynamicSQLManager;
import org.azkfw.dsql.Group;
import org.azkfw.dsql.Parameter;
import org.azkfw.persistence.database.DatabaseConnection;
import org.azkfw.persistence.database.DatabaseConnectionSupport;

/**
 * このクラスは、DynamicSQLのテストをサポートしたテストクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2015/02/17
 * @author Kawakicchi
 */
public class AbstractDynamicSQLTestCase extends AbstractDatasourceTestCase {

	private String dynamicSQLName;

	private List<String> sqls;

	@Override
	public void setUp() {
		super.setUp();

		sqls = new ArrayList<String>();

		try {
			dynamicSQLName = null;
			Class<?> clazz = this.getClass();
			Method method = clazz.getMethod(getTestName().getMethodName());
			org.azkfw.business.test.annotation.DynamicSQL dynamicSQL = method.getAnnotation(org.azkfw.business.test.annotation.DynamicSQL.class);
			if (null != dynamicSQL) {
				dynamicSQLName = dynamicSQL.value();
			}
			if (null == dynamicSQLName || 0 == dynamicSQLName.length()) {
				dynamicSQL = clazz.getAnnotation(org.azkfw.business.test.annotation.DynamicSQL.class);
				if (null != dynamicSQL) {
					dynamicSQLName = dynamicSQL.value();
				}
			}

			if (null == dynamicSQL || 0 == dynamicSQLName.length()) {
				fail("Undefined DynamicSQL.");
			}
		} catch (NoSuchMethodException ex) {
			fatal("", ex);
			fail("");
		}
	}

	@Override
	public void tearDown() {

		super.tearDown();
	}

	/**
	 * カウント処理を実行する。
	 * 
	 * @return 件数
	 */
	protected final long count() {
		return doCount(null, null);
	}

	/**
	 * カウント処理を実行する。
	 * 
	 * @param group グループ
	 * @return 件数
	 */
	protected final long count(final Group group) {
		return doCount(group, null);
	}

	/**
	 * カウント処理を実行する。
	 * 
	 * @param parameter パラメータ
	 * @return 件数
	 */
	protected final long count(final Parameter parameter) {
		return doCount(null, parameter);
	}

	/**
	 * カウント処理を実行する。
	 * 
	 * @param group グループ
	 * @param parameter パラメータ
	 * @return 件数
	 */
	protected final long count(final Group group, final Parameter parameter) {
		return doCount(group, parameter);
	}

	/**
	 * クエリー処理を実行する。
	 * 
	 * @return クエリー結果
	 */
	protected final List<Map<String, Object>> query() {
		return doQuery(null, null);
	}

	/**
	 * クエリー処理を実行する。
	 * 
	 * @param group グループ
	 * @return クエリー結果
	 */
	protected final List<Map<String, Object>> query(final Group group) {
		return doQuery(group, null);
	}

	/**
	 * クエリー処理を実行する。
	 * 
	 * @param parameter パラメータ
	 * @return クエリー結果
	 */
	protected final List<Map<String, Object>> query(final Parameter parameter) {
		return doQuery(null, parameter);
	}

	/**
	 * クエリー処理を実行する。
	 * 
	 * @param group グループ
	 * @param parameter パラメータ
	 * @return クエリー結果
	 */
	protected final List<Map<String, Object>> query(final Group group, final Parameter parameter) {
		return doQuery(group, parameter);
	}

	/**
	 * 処理を実行する。
	 * 
	 * @return 結果
	 */
	protected final boolean execute() {
		return doExecute(null, null);
	}

	/**
	 * 処理を実行する。
	 * 
	 * @param group グループ
	 * @return 結果
	 */
	protected final boolean execute(final Group group) {
		return doExecute(group, null);
	}

	/**
	 * 処理を実行する。
	 * 
	 * @param parameter パラメータ
	 * @return 結果
	 */
	protected final boolean execute(final Parameter parameter) {
		return doExecute(null, parameter);
	}

	/**
	 * 処理を実行する。
	 * 
	 * @param group グループ
	 * @param parameter パラメータ
	 * @return 結果
	 */
	protected final boolean execute(final Group group, final Parameter parameter) {
		return doExecute(group, parameter);
	}

	/**
	 * 更新処理を実行する。
	 * 
	 * @return 更新件数
	 */
	protected final int update() {
		return doUpdate(null, null);
	}

	/**
	 * 更新処理を実行する。
	 * 
	 * @param group グループ
	 * @return 更新件数
	 */
	protected final int update(final Group group) {
		return doUpdate(group, null);
	}

	/**
	 * 更新処理を実行する。
	 * 
	 * @param parameter パラメータ
	 * @return 更新件数
	 */
	protected final int update(final Parameter parameter) {
		return doUpdate(null, parameter);
	}

	/**
	 * 更新処理を実行する。
	 * 
	 * @param group グループ
	 * @param parameter パラメータ
	 * @return 更新件数
	 */
	protected final int update(final Group group, final Parameter parameter) {
		return doUpdate(group, parameter);
	}

	private long doCount(final Group group, final Parameter parameter) {
		DynamicSQL dsql = DynamicSQLManager.generate(dynamicSQLName, group, parameter);
		assertNotNull("Undefined DynamicSQL.[" + dynamicSQLName + "]", dsql);

		Connection connection = null;
		long result = -1;
		try {
			DynamicSQLAccessObject dao = new DynamicSQLAccessObject(dsql);
			if (dao instanceof DatabaseConnectionSupport) {
				connection = getConnection();
				((DatabaseConnectionSupport) dao).setConnection(new DatabaseConnection(connection));
			}

			result = dao.count();

			sqls.add(dsql.getExecuteSQL());

		} catch (DataAccessServiceException ex) {
			fatal(ex);
			fail(String.format("DynamicSQL count error.[%s]", dsql.getExecuteSQL()));
		} finally {
			releaseConnection(connection);
		}
		return result;
	}

	private List<Map<String, Object>> doQuery(final Group group, final Parameter parameter) {
		DynamicSQL dsql = DynamicSQLManager.generate(dynamicSQLName, group, parameter);
		assertNotNull("Undefined DynamicSQL.[" + dynamicSQLName + "]", dsql);

		Connection connection = null;
		List<Map<String, Object>> records = null;
		try {
			DynamicSQLAccessObject dao = new DynamicSQLAccessObject(dsql);
			if (dao instanceof DatabaseConnectionSupport) {
				connection = getConnection();
				((DatabaseConnectionSupport) dao).setConnection(new DatabaseConnection(connection));
			}

			records = dao.query();

			sqls.add(dsql.getExecuteSQL());

		} catch (DataAccessServiceException ex) {
			fatal(ex);
			fail(String.format("DynamicSQL query error.[%s]", dsql.getExecuteSQL()));
		} finally {
			releaseConnection(connection);
		}
		return records;
	}

	private boolean doExecute(final Group group, final Parameter parameter) {
		DynamicSQL dsql = DynamicSQLManager.generate(dynamicSQLName, group, parameter);
		assertNotNull("Undefined DynamicSQL.[" + dynamicSQLName + "]", dsql);

		Connection connection = null;
		boolean result = false;
		try {
			DynamicSQLAccessObject dao = new DynamicSQLAccessObject(dsql);
			if (dao instanceof DatabaseConnectionSupport) {
				connection = getConnection();
				((DatabaseConnectionSupport) dao).setConnection(new DatabaseConnection(connection));
			}

			result = dao.execute();

			sqls.add(dsql.getExecuteSQL());

		} catch (DataAccessServiceException ex) {
			fatal(ex);
			fail(String.format("DynamicSQL execute error.[%s]", dsql.getExecuteSQL()));
		} finally {
			releaseConnection(connection);
		}
		return result;
	}

	private int doUpdate(final Group group, final Parameter parameter) {
		DynamicSQL dsql = DynamicSQLManager.generate(dynamicSQLName, group, parameter);
		assertNotNull("Undefined DynamicSQL.[" + dynamicSQLName + "]", dsql);

		Connection connection = null;
		int result = -1;
		try {
			DynamicSQLAccessObject dao = new DynamicSQLAccessObject(dsql);
			if (dao instanceof DatabaseConnectionSupport) {
				connection = getConnection();
				((DatabaseConnectionSupport) dao).setConnection(new DatabaseConnection(connection));
			}

			result = dao.update();

			sqls.add(dsql.getExecuteSQL());

		} catch (DataAccessServiceException ex) {
			fatal(ex);
			fail(String.format("DynamicSQL update error.[%s]", dsql.getExecuteSQL()));
		} finally {
			releaseConnection(connection);
		}
		return result;
	}
}
