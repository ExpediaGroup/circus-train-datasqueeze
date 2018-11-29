/**
 * Copyright (C) 2018 Expedia Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expedia.dsp.circustrain.datasqueeze;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyMapOf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expedia.dsp.data.squeeze.CompactionManager;
import com.expedia.dsp.data.squeeze.CompactionManagerFactory;
import com.expedia.dsp.data.squeeze.models.CompactionCriteria;
import com.expedia.dsp.data.squeeze.models.CompactionResponse;
import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

/**
 * Tests for {@link DataSqueezeCopier}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ CompactionManagerFactory.class, FileSystem.class, CompactionCriteria.class, CompactionManager.class })
public final class DataSqueezeCopierTest {

    private final Configuration conf = new Configuration();
    private final Path sourceDataLocation = new Path("source");
    private final Path replicaDataLocation = new Path("destination");

    @Test
    public void copyTest() throws Exception {
        final DataSqueezeOptions options = new DataSqueezeOptions();
        final CompactionManager compactionManager = PowerMockito.mock(CompactionManager.class);
        final CompactionResponse compactionResponse = PowerMockito.mock(CompactionResponse.class);
        PowerMockito.mockStatic(CompactionManagerFactory.class);
        PowerMockito.when(compactionResponse.isSuccessful()).thenReturn(true);
        PowerMockito.when(compactionManager.compact()).thenReturn(compactionResponse);
        PowerMockito.when(CompactionManagerFactory.create(anyMapOf(String.class, String.class))).thenReturn(compactionManager);
        PowerMockito.mockStatic(FileSystem.class);
        final FileSystem fileSystem = PowerMockito.mock(FileSystem.class);
        PowerMockito.when(fileSystem.delete(this.sourceDataLocation, true)).thenReturn(true);
        PowerMockito.when(FileSystem.get(this.sourceDataLocation.toUri(), this.conf)).thenReturn(fileSystem);

        final DataSqueezeCopier copier = new DataSqueezeCopier(this.conf, this.sourceDataLocation, this.replicaDataLocation, options);
        final Metrics metrics = copier.copy();
        assertThat(metrics.getMetrics().containsKey(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(true));
        assertThat(metrics.getMetrics().get(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(1L));
    }


    @Test(expected = CircusTrainException.class)
    public void copyTestThrowsCircusTrainException() throws Exception {
        final DataSqueezeOptions options = new DataSqueezeOptions();
        final CompactionManager compactionManager = PowerMockito.mock(CompactionManager.class);
        final CompactionResponse compactionResponse = PowerMockito.mock(CompactionResponse.class);
        PowerMockito.mockStatic(CompactionManagerFactory.class);
        PowerMockito.when(compactionResponse.isSuccessful()).thenReturn(true);
        PowerMockito.when(compactionManager.compact()).thenThrow(Exception.class);
        PowerMockito.when(CompactionManagerFactory.create(anyMapOf(String.class, String.class))).thenReturn(compactionManager);
        PowerMockito.mockStatic(FileSystem.class);
        final FileSystem fileSystem = PowerMockito.mock(FileSystem.class);
        PowerMockito.when(fileSystem.delete(this.sourceDataLocation, true)).thenReturn(true);
        PowerMockito.when(FileSystem.get(this.sourceDataLocation.toUri(), this.conf)).thenReturn(fileSystem);

        final DataSqueezeCopier copier = new DataSqueezeCopier(this.conf, this.sourceDataLocation, this.replicaDataLocation, options);
        final Metrics metrics = copier.copy();
        assertThat(metrics.getMetrics().containsKey(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(true));
        assertThat(metrics.getMetrics().get(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(0L));
    }

}
