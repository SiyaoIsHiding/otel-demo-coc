package org.example;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.type.codec.BigIntCodec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;


public class Main {
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .withOpenTelemetry(initOpenTelemetry())
                .build()) {
            for (int i = 0; i < 1000; i++) {
                session.execute(SimpleStatement.newInstance("select * from system.peers").setIdempotent(true)); // setTracing(true) if you want to see Cassandra internal tracing
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static OpenTelemetry initOpenTelemetry() {

        ManagedChannel jaegerChannel =
                ManagedChannelBuilder.forAddress("localhost", 14250).usePlaintext().build();

        JaegerGrpcSpanExporter jaegerExporter =
                JaegerGrpcSpanExporter.builder()
                        .setChannel(jaegerChannel)
                        .setTimeout(30, TimeUnit.SECONDS)
                        .build();

        Resource serviceNameResource =
                Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "Demo App"));

        SdkTracerProvider tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(jaegerExporter))
                        .setResource(Resource.getDefault().merge(serviceNameResource))
                        .build();
        OpenTelemetrySdk openTelemetry =
                OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

        Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::shutdown));

        return openTelemetry;
    }
}