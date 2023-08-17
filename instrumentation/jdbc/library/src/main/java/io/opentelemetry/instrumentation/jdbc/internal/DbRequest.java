/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.jdbc.internal;

import static io.opentelemetry.instrumentation.jdbc.internal.JdbcUtils.connectionFromStatement;
import static io.opentelemetry.instrumentation.jdbc.internal.JdbcUtils.extractDbInfo;

import com.google.auto.value.AutoValue;
import io.opentelemetry.instrumentation.jdbc.internal.dbinfo.DbInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
@AutoValue
public abstract class DbRequest {

  @Nullable
  public static DbRequest create(PreparedStatement statement, Map<Integer, Object> paramValues) {
    return create(statement, JdbcData.preparedStatement.get(statement), paramValues);
  }

  @Nullable
  public static DbRequest create(
      Statement statement, String dbStatementString, Map<Integer, Object> paramValues) {
    Connection connection = connectionFromStatement(statement);
    if (connection == null) {
      return null;
    }

    return create(extractDbInfo(connection), dbStatementString, paramValues);
  }

  public static DbRequest create(
      DbInfo dbInfo, String statement, Map<Integer, Object> paramValues) {
    return new AutoValue_DbRequest(dbInfo, statement, paramValues);
  }

  public abstract DbInfo getDbInfo();

  @Nullable
  public abstract String getStatement();

  public abstract Map<Integer, Object> getParamValues();
}
