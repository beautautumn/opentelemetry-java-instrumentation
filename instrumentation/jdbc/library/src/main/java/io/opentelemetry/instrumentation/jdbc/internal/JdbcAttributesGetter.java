/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.jdbc.internal;

import io.opentelemetry.instrumentation.api.instrumenter.db.SqlClientAttributesGetter;
import io.opentelemetry.instrumentation.jdbc.internal.dbinfo.DbInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
public final class JdbcAttributesGetter implements SqlClientAttributesGetter<DbRequest> {

  @Nullable
  @Override
  public String getSystem(DbRequest request) {
    return request.getDbInfo().getSystem();
  }

  @Nullable
  @Override
  public String getUser(DbRequest request) {
    return request.getDbInfo().getUser();
  }

  @Nullable
  @Override
  public String getName(DbRequest request) {
    DbInfo dbInfo = request.getDbInfo();
    return dbInfo.getName() == null ? dbInfo.getDb() : dbInfo.getName();
  }

  @Nullable
  @Override
  public String getConnectionString(DbRequest request) {
    return request.getDbInfo().getShortUrl();
  }

  @Nullable
  @Override
  public String getRawStatement(DbRequest request) {
    return request.getStatement();
  }

  @Nullable
  @Override
  public String getParamValues(DbRequest dbRequest) {
    List<String> params = new ArrayList<>();
    for (Map.Entry<Integer, Object> paramEntity : dbRequest.getParamValues().entrySet()) {
      params.add(
          String.format("index: %d, value: %s", paramEntity.getKey(), paramEntity.getValue()));
    }
    return String.join(",", params);
  }
}
