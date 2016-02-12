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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.azkfw.datasource.Datasource;
import org.azkfw.datasource.Field;
import org.azkfw.datasource.FieldType;
import org.azkfw.datasource.Record;
import org.azkfw.datasource.Table;
import org.azkfw.datasource.excel.ExcelDatasourceBuilder;
import org.azkfw.util.StringUtility;

/**
 * このクラスは、データソース機能をサポートしたテストクラスです。
 * 
 * @since 1.0.0
 * @version 1.0.0 2015/01/27
 * @author Kawakicchi
 */
public class AbstractDatasourceTestCase extends AbstractDatabaseTestCase {

	private static Class<? extends TestCase> TEST_CLASS = null;
	private static Datasource INIT_DATASOURCE = null;
	private static Datasource TEST_DATASOURCE = null;
	private static Map<String, Datasource> CASH_DATASOURCES = new HashMap<String, Datasource>();

	@Override
	public void setUp() {
		super.setUp();

		if (null == TEST_CLASS || !TEST_CLASS.equals(this.getClass())) {
			TEST_CLASS = this.getClass();

			// Load init datasource
			InitDatasourceFile id = TEST_CLASS.getAnnotation(InitDatasourceFile.class);
			if (null != id && StringUtility.isNotEmpty(id.value())) {
				Datasource ds = null;
				if (CASH_DATASOURCES.containsKey(id.value())) {
					debug(String.format("Use cash datasource.[%s]", id.value()));
					ds = CASH_DATASOURCES.get(id.value());
				} else {
					ds = getTestFileToDatasource(id.value());
					CASH_DATASOURCES.put(id.value(), ds);
				}
				INIT_DATASOURCE = ds;
			}
			// Store init datasource
			if (null != INIT_DATASOURCE) {
				storeDatabase(INIT_DATASOURCE);
			}

			// Load test datasource
			TestDatasourceFile td = TEST_CLASS.getAnnotation(TestDatasourceFile.class);
			if (null != td && StringUtility.isNotEmpty(td.value())) {
				Datasource ds = null;
				if (CASH_DATASOURCES.containsKey(td.value())) {
					debug(String.format("Use cash datasource.[%s]", td.value()));
					ds = CASH_DATASOURCES.get(td.value());
				} else {
					ds = getTestFileToDatasource(td.value());
					CASH_DATASOURCES.put(td.value(), ds);
				}
				TEST_DATASOURCE = ds;
			}
		}

		// Store test datasource
		if (null != TEST_DATASOURCE) {
			storeDatabase(TEST_DATASOURCE);
		}
	}

	@Override
	public void tearDown() {

		super.tearDown();
	}

	/**
	 * テストファイルをデータソースとして取得する。
	 * 
	 * @param name 名前
	 * @return データソース
	 */
	protected final Datasource getTestFileToDatasource(final String name) {
		Datasource ds = null;
		try {
			InputStream is = getTestContext().getResourceAsStream(name);
			if (null != is) {
				ExcelDatasourceBuilder builder = ExcelDatasourceBuilder.newInstance(name);
				builder.addInputStream(is);
				ds = builder.build();
			} else {
				fatal(String.format("Not found datasource file.[%s]", name));
				fail(String.format("Not found datasource file.[%s]", name));
			}
		} catch (Exception ex) {
			fatal(ex);
			fail(String.format("Datasource load error.[%s]", name));
		}
		return ds;
	}

	/**
	 * データソースの内容を比較する。
	 * 
	 * @param expected 期待値
	 * @param actual 現行値
	 */
	public static void assertEquals(final Datasource expected, final Datasource actual) {
		assertEquals(null, expected, actual, null);
	}

	/**
	 * データソースの内容を比較する。
	 * 
	 * @param expected 期待値
	 * @param actual 現行値
	 * @param option 比較オプション
	 */
	public static void assertEquals(final Datasource expected, final Datasource actual, final DatasourceAssertOption option) {
		assertEquals(null, expected, actual, option);
	}

	/**
	 * データソースの内容を比較する。
	 * 
	 * @param message メッセージ
	 * @param expected 期待値
	 * @param actual 現行値
	 */
	public static void assertEquals(final String message, final Datasource expected, final Datasource actual) {
		assertEquals(message, expected, actual, null);
	}

	/**
	 * データソースの内容を比較する。
	 * 
	 * @param message メッセージ
	 * @param expected 期待値
	 * @param actual 現行値
	 * @param option 比較オプション
	 */
	public static void assertEquals(final String message, final Datasource expected, final Datasource actual, final DatasourceAssertOption option) {
		List<Table> expTables = expected.getTables();
		List<Table> actTables = actual.getTables();
		for (Table expTable : expTables) {
			Table actTable = null;
			for (Table tbl : actTables) {
				if (expTable.getName().equals(tbl.getName())) {
					actTable = tbl;
					break;
				}
			}
			if (null == actTable) {
				fail(String.format("Not found table.[%s]", expTable.getName()));
			}
			assertEquals(message, expTable, actTable, option);
		}
	}

	/**
	 * テーブルの内容を比較する。
	 * 
	 * @param message メッセージ
	 * @param expected 期待値
	 * @param actual 現行値
	 * @param option 比較オプション
	 */
	public static void assertEquals(final String message, final Table expected, final Table actual, final DatasourceAssertOption option) {
		// TODO: テーブル比較処理

	}

	private void storeDatabase(final Datasource datasource) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = getConnection();

			List<Table> tables = datasource.getTables();
			// delete
			for (int i = 0; i < tables.size(); i++) {
				Table table = tables.get(i);
				ps = connection.prepareStatement(getDeleteSQL(table));
				int size = ps.executeUpdate();
				info(String.format("Table delete data.[%s, %d]", table.getName(), size));
				ps.close();
				ps = null;
			}
			// insert
			for (int i = tables.size() - 1; i >= 0; i--) {
				Table table = tables.get(i);
				List<Field> fields = table.getFields();
				List<Record> records = table.getRecords();
				if (0 < records.size()) {
					ps = connection.prepareStatement(getInsertSQL(table));
					int index = 1;
					for (int j = 0; j < records.size(); j++) {
						Record record = records.get(j);
						for (int k = 0; k < fields.size(); k++) {
							Field field = fields.get(k);
							Object value = record.get(field.getName());
							if (null == value) {
								ps.setObject(index, value);
							} else if (FieldType.Date == field.getType()) {
								Date date = null;
								if (value instanceof Date) {
									value = date;
								} else if (value instanceof java.util.Date) {
									Timestamp ts = new Timestamp(((java.util.Date) value).getTime());
									date = new Date(ts.getTime());
								}
								ps.setObject(index, date);
							} else {
								ps.setObject(index, value);
							}
							index++;
						}
					}
					int size = ps.executeUpdate();
					info(String.format("Table insert data.[%s, %d]", table.getName(), size));
					ps.close();
					ps = null;
				}
			}

			connection.commit();

		} catch (SQLException ex) {
			ex.printStackTrace();
			fail("Datasource store error.");
		} finally {
			if (null != ps) {
				try {
					ps.close();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			if (null != connection) {
				try {
					connection.close();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	private String getDeleteSQL(final Table table) {
		return String.format("DELETE FROM %s;", table.getName());
	}

	private String getInsertSQL(final Table table) {
		List<Field> fields = table.getFields();

		StringBuilder values = new StringBuilder();
		values.append("(");
		for (int j = 0; j < fields.size(); j++) {
			if (0 != j) {
				values.append(", ");
			}
			values.append("?");
		}
		values.append(")");

		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ");
		sql.append(table.getName());
		sql.append("(");
		for (int i = 0; i < fields.size(); i++) {
			Field field = fields.get(i);
			if (0 != i) {
				sql.append(", ");
			}
			sql.append(field.getName());
		}
		sql.append(") VALUES ");

		for (int i = 0; i < table.getRecords().size(); i++) {
			if (0 != i) {
				sql.append(", ");
			}
			sql.append(values.toString());
		}
		return sql.toString();
	}
}
