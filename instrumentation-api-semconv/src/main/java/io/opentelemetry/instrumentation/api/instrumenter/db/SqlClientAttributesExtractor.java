/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.instrumenter.db;

import static io.opentelemetry.instrumentation.api.internal.AttributesExtractorUtil.internalSet;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.db.SqlStatementInfo;
import io.opentelemetry.instrumentation.api.db.SqlStatementSanitizer;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;

/**
 * Extractor of <a
 * href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/database.md">database
 * attributes</a>. This class is designed with SQL (or SQL-like) database clients in mind.
 *
 * <p>It sets the same set of attributes as {@link DbClientAttributesExtractor} plus an additional
 * <code>{@linkplain SemanticAttributes#DB_SQL_TABLE db.sql.table}</code> attribute. The raw SQL
 * statements returned by the {@link SqlClientAttributesGetter#getRawStatement(Object)} method are
 * sanitized before use, all statement parameters are removed.
 */
public final class SqlClientAttributesExtractor<REQUEST, RESPONSE>
    extends DbClientCommonAttributesExtractor<
        REQUEST, RESPONSE, SqlClientAttributesGetter<REQUEST>> {

  /** Creates the SQL client attributes extractor with default configuration. */
  public static <REQUEST, RESPONSE> AttributesExtractor<REQUEST, RESPONSE> create(
      SqlClientAttributesGetter<REQUEST> getter) {
    return SqlClientAttributesExtractor.<REQUEST, RESPONSE>builder(getter).build();
  }

  /**
   * Returns a new {@link SqlClientAttributesExtractorBuilder} that can be used to configure the SQL
   * client attributes extractor.
   */
  public static <REQUEST, RESPONSE> SqlClientAttributesExtractorBuilder<REQUEST, RESPONSE> builder(
      SqlClientAttributesGetter<REQUEST> getter) {
    return new SqlClientAttributesExtractorBuilder<>(getter);
  }

  private static final String SQL_CALL = "CALL";

  private final AttributeKey<String> dbTableAttribute;
  private final SqlStatementSanitizer sanitizer;

  SqlClientAttributesExtractor(
      SqlClientAttributesGetter<REQUEST> getter,
      AttributeKey<String> dbTableAttribute,
      SqlStatementSanitizer sanitizer) {
    super(getter);
    this.dbTableAttribute = dbTableAttribute;
    this.sanitizer = sanitizer;
  }

  @Override
  public void onStart(AttributesBuilder attributes, Context parentContext, REQUEST request) {
    super.onStart(attributes, parentContext, request);

    SqlStatementInfo sanitizedStatement = sanitizer.sanitize(getter.getRawStatement(request));
    String operation = sanitizedStatement.getOperation();
    internalSet(attributes, SemanticAttributes.DB_STATEMENT, sanitizedStatement.getFullStatement());
    internalSet(attributes, SemanticAttributes.DB_OPERATION, operation);
    String paramValue = getter.getParamValues(request);
    if (paramValue != null && paramValue.length() > 0) {
      internalSet(attributes, AttributeKey.stringKey("db.statement.values"), paramValue);
    }
    if (!SQL_CALL.equals(operation)) {
      internalSet(attributes, dbTableAttribute, sanitizedStatement.getMainIdentifier());
    }
  }
}
