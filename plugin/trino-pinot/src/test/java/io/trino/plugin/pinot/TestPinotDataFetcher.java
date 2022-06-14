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
package io.trino.plugin.pinot;

import io.trino.plugin.pinot.client.PinotGrpcDataFetcher;
import io.trino.testing.assertions.Assert;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableImplV3;
import org.testng.annotations.Test;

import java.util.Map;

public class TestPinotDataFetcher
{
    class DataTableWithMetadata
            extends DataTableImplV3
    {
        private final Map<String, String> metadata;

        public DataTableWithMetadata(Map<String, String> metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public Map<String, String> getMetadata()
        {
            return metadata;
        }
    }

    @Test
    public void testPinotDataTableException()
    {
        DataTable dataTable = new DataTableWithMetadata(Map.of(DataTable.EXCEPTION_METADATA_KEY + "1", "exception 1", DataTable.EXCEPTION_METADATA_KEY + "2", "exception 2"));
        try {
            new PinotGrpcDataFetcher(null, null, null, null).checkExceptions(dataTable, null, null);
            Assert.fail("PinotDataFetcher checkExceptions method should throw exception");
        }
        catch (PinotException pinotException) {
            Assert.assertEquals(pinotException.getMessage(), "");
        }
    }
}
