/**
 * Copyright (C) 2018 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;

/**
 * Tests for {@link DataSqueezeMetrics}
 */
public final class DataSqueezeMetricsTest {

    private DataSqueezeMetrics compactionMetrics;

    @Test
    public void testGetBytesReplicated() {
        this.compactionMetrics = new DataSqueezeMetrics();
        assertThat(this.compactionMetrics.getBytesReplicated(), is(-1L));
    }

    @Test
    public void testGetMetrics() {
        this.compactionMetrics = new DataSqueezeMetrics();
        assertThat(this.compactionMetrics.getMetrics(), is(instanceOf(Map.class)));
    }

    @Test
    public void testSetCompactionSuccess() {
        this.compactionMetrics = new DataSqueezeMetrics();
        this.compactionMetrics.setCompactionSuccess(true);
        assertThat(this.compactionMetrics.getMetrics(), is(instanceOf(Map.class)));
        assertThat(this.compactionMetrics.getMetrics().containsKey(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(true));
        assertThat(this.compactionMetrics.getMetrics().get(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(1L));
    }

    @Test
    public void testSetCompactionSuccessFalse() {
        this.compactionMetrics = new DataSqueezeMetrics();
        this.compactionMetrics.setCompactionSuccess(false);
        assertThat(this.compactionMetrics.getMetrics(), is(instanceOf(Map.class)));
        assertThat(this.compactionMetrics.getMetrics().containsKey(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(true));
        assertThat(this.compactionMetrics.getMetrics().get(DataSqueezeMetrics.METRIC_COMPACTION_SUCCESS), is(0L));
    }

}
