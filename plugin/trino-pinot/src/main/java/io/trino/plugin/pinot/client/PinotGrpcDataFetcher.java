/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.pinot.client;

import com.google.common.collect.AbstractIterator;
import com.google.common.net.HostAndPort;
import io.trino.plugin.pinot.PinotConfig;
import io.trino.plugin.pinot.PinotErrorCode;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotSplit;
import io.trino.spi.connector.ConnectorSession;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class PinotGrpcDataFetcher
        implements PinotDataFetcher
{
    private final PinotSplit split;
    private final PinotServerQueryClient pinotGrpcClient;
    private final String query;
    private long readTimeNanos;
    private Iterator<PinotDataTableWithSize> responseIterator;
    private boolean isPinotDataFetched;
    private final RowCountChecker rowCountChecker;
    private long estimatedMemoryUsageInBytes;

    public PinotGrpcDataFetcher(PinotServerQueryClient pinotGrpcClient, PinotSplit split, String query, RowCountChecker rowCountChecker)
    {
        this.pinotGrpcClient = requireNonNull(pinotGrpcClient, "pinotGrpcClient is null");
        this.split = requireNonNull(split, "split is null");
        this.query = requireNonNull(query, "query is null");
        this.rowCountChecker = requireNonNull(rowCountChecker, "rowCountChecker is null");
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsageBytes()
    {
        return estimatedMemoryUsageInBytes;
    }

    @Override
    public boolean endOfData()
    {
        return !responseIterator.hasNext();
    }

    @Override
    public boolean isDataFetched()
    {
        return isPinotDataFetched;
    }

    @Override
    public void fetchData()
    {
        long startTimeNanos = System.nanoTime();
        String serverHost = split.getSegmentHost().orElseThrow(() -> new PinotException(PinotErrorCode.PINOT_INVALID_PQL_GENERATED, Optional.empty(), "Expected the segment split to contain the host"));
        this.responseIterator = pinotGrpcClient.queryPinot(null, query, serverHost, split.getSegments());
        readTimeNanos += System.nanoTime() - startTimeNanos;
        isPinotDataFetched = true;
    }

    @Override
    public PinotDataTableWithSize getNextDataTable()
    {
        PinotDataTableWithSize dataTableWithSize = responseIterator.next();
        estimatedMemoryUsageInBytes = dataTableWithSize.getEstimatedSizeInBytes();
        rowCountChecker.checkTooManyRows(dataTableWithSize.getDataTable());
        checkExceptions(dataTableWithSize.getDataTable(), split, query);
        return dataTableWithSize;
    }

    public static class Factory
            implements PinotDataFetcher.Factory
    {
        private final PinotServerQueryClient queryClient;
        private final int limitForSegmentQueries;

        @Inject
        public Factory(PinotHostMapper pinotHostMapper, PinotConfig pinotConfig)
        {
            requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            requireNonNull(pinotConfig, "pinotConfig is null");
            this.limitForSegmentQueries = requireNonNull(pinotConfig, "pinotConfig is null").getMaxRowsPerSplitForSegmentQueries();
            this.queryClient = new PinotGrpcServerQueryClientClient(pinotHostMapper, pinotConfig);
        }

        @Override
        public PinotDataFetcher create(ConnectorSession session, String query, PinotSplit split)
        {
            return new PinotGrpcDataFetcher(queryClient, split, query, new RowCountChecker(limitForSegmentQueries, query));
        }
    }

    public static class PinotGrpcServerQueryClientClient
            implements PinotServerQueryClient
    {
        private static final CalciteSqlCompiler REQUEST_COMPILER = new CalciteSqlCompiler();

        private final PinotHostMapper pinotHostMapper;
        private final Map<HostAndPort, GrpcQueryClient> clientCache = new ConcurrentHashMap<>();
        private final int grpcPort;

        public PinotGrpcServerQueryClientClient(PinotHostMapper pinotHostMapper, PinotConfig pinotConfig)
        {
            this.pinotHostMapper = requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            this.grpcPort = requireNonNull(pinotConfig, "pinotConfig is null").getGrpcPort();
        }

        @Override
        public Iterator<PinotDataTableWithSize> queryPinot(ConnectorSession session, String query, String serverHost, List<String> segments)
        {
            HostAndPort mappedHostAndPort = pinotHostMapper.getServerGrpcHostAndPort(serverHost, grpcPort);
            GrpcQueryClient client = clientCache.computeIfAbsent(mappedHostAndPort, hostAndPort -> new GrpcQueryClient(hostAndPort.getHost(), hostAndPort.getPort()));
            BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
            GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder()
                    .setSql(query)
                    .setSegments(segments)
                    .setEnableStreaming(true)
                    .setBrokerRequest(brokerRequest);
            return new ResponseIterator(client.submit(requestBuilder.build()));
        }

        public static class ResponseIterator
                extends AbstractIterator<PinotDataTableWithSize>
        {
            private final Iterator<Server.ServerResponse> responseIterator;

            public ResponseIterator(Iterator<Server.ServerResponse> responseIterator)
            {
                this.responseIterator = requireNonNull(responseIterator, "responseIterator is null");
            }

            @Override
            protected PinotDataTableWithSize computeNext()
            {
                if (!responseIterator.hasNext()) {
                    return endOfData();
                }
                Server.ServerResponse response = responseIterator.next();
                String responseType = response.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE);
                if (responseType.equals(CommonConstants.Query.Response.ResponseType.METADATA)) {
                    return endOfData();
                }
                ByteBuffer buffer = response.getPayload().asReadOnlyByteBuffer();
                try {
                    return new PinotDataTableWithSize(DataTableFactory.getDataTable(buffer), buffer.remaining());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
