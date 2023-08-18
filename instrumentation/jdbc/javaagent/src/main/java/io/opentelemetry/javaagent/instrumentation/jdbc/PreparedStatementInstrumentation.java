/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jdbc;

import static io.opentelemetry.javaagent.bootstrap.Java8BytecodeBridge.currentContext;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.implementsInterface;
import static io.opentelemetry.javaagent.instrumentation.jdbc.JdbcSingletons.statementInstrumenter;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.jdbc.internal.DbRequest;
import io.opentelemetry.javaagent.bootstrap.CallDepth;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

public class PreparedStatementInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<ClassLoader> classLoaderOptimization() {
    return hasClassesNamed("java.sql.PreparedStatement");
  }

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return implementsInterface(named("java.sql.PreparedStatement"));
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        nameStartsWith("set")
            .and(isPublic())
            .or(nameStartsWith("execute").and(takesArguments(0)).and(isPublic())),
        PreparedStatementInstrumentation.class.getName() + "$PreparedStatementAdvice");
  }

  @SuppressWarnings("unused")
  public static class PreparedStatementAdvice {
    public static final ThreadLocal<Map<Integer, Object>> paramValuesThreadLocal =
        new ThreadLocal<>();

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.This PreparedStatement statement,
        @Advice.Origin String methodName,
        @Advice.AllArguments(nullIfEmpty = true) Object[] args,
        @Advice.Local("otelCallDepth") CallDepth callDepth,
        @Advice.Local("otelRequest") DbRequest request,
        @Advice.Local("otelContext") Context context,
        @Advice.Local("otelScope") Scope scope) {
      // Connection#getMetaData() may execute a Statement or PreparedStatement to retrieve DB info
      // this happens before the DB CLIENT span is started (and put in the current context), so this
      // instrumentation runs again and the shouldStartSpan() check always returns true - and so on
      // until we get a StackOverflowError
      // using CallDepth prevents this, because this check happens before Connection#getMetadata()
      // is called - the first recursive Statement call is just skipped and we do not create a span
      // for it
      String longMethodName = methodName.substring(0, methodName.indexOf("("));
      String shortMethodName = longMethodName.substring(longMethodName.lastIndexOf(".") + 1);
      if (shortMethodName.startsWith("set") && (args != null) && (args.length > 1)) {

        getParamValuesThreadLocal().put((Integer) args[0], args[1]);

      } else if (shortMethodName.startsWith("execute")) {

        callDepth = CallDepth.forClass(Statement.class);
        if (callDepth.getAndIncrement() > 0) {
          return;
        }

        Context parentContext = currentContext();
        request = DbRequest.create(statement, getParamValuesThreadLocal());

        if (request == null || !statementInstrumenter().shouldStart(parentContext, request)) {
          return;
        }

        context = statementInstrumenter().start(parentContext, request);
        scope = context.makeCurrent();
      }
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void stopSpan(
        @Advice.Origin String methodName,
        @Advice.Thrown Throwable throwable,
        @Advice.Local("otelCallDepth") CallDepth callDepth,
        @Advice.Local("otelRequest") DbRequest request,
        @Advice.Local("otelContext") Context context,
        @Advice.Local("otelScope") Scope scope) {

      String longMethodName = methodName.substring(0, methodName.indexOf("("));
      String shortMethodName = longMethodName.substring(longMethodName.lastIndexOf(".") + 1);
      if (!shortMethodName.startsWith("execute")) {
        return;
      }

      if (callDepth.decrementAndGet() > 0) {
        return;
      }

      if (scope != null) {
        scope.close();
        statementInstrumenter().end(context, request, null, throwable);
      }

      getParamValuesThreadLocal().clear();
    }

    public static Map<Integer, Object> getParamValuesThreadLocal() {
      Map<Integer, Object> paramValues = paramValuesThreadLocal.get();
      if (paramValues == null) {
        paramValues = new HashMap<>();
        paramValuesThreadLocal.set(paramValues);
      }
      return paramValues;
    }
  }
}
