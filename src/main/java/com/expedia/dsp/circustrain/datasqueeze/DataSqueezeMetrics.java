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

import java.util.HashMap;
import java.util.Map;

import com.hotels.bdp.circustrain.api.metrics.Metrics;

/**
 * Metrics for the DataSqueezeCopier.
 */
public class DataSqueezeMetrics implements Metrics {

    static final String METRIC_COMPACTION_SUCCESS = DataSqueezeCopier.class + ".datasqueeze.success";

    private final Map<String, Long> metrics = new HashMap<>();

    /**
     * Set the compaction status
     *
     * @param successful Whether or not compaction was successful
     */
    public void setCompactionSuccess(final boolean successful) {
        this.metrics.put(METRIC_COMPACTION_SUCCESS, successful ? 1L : 0);
    }

    @Override
    public Map<String, Long> getMetrics() {
        return this.metrics;
    }

    @Override
    public long getBytesReplicated() {
        return -1;
    }
}
