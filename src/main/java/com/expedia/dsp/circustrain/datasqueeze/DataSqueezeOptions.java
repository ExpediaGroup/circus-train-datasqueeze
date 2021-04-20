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

import java.util.Map;

import org.apache.commons.collections.MapUtils;

/**
 * Contains options for the DataSqueezeCopier.
 *
 * Initializes itself from a Map of values.
 */
public class DataSqueezeOptions {

    public static final String THRESHOLD = "threshold";

    /**
     * Threshold in bytes for datasqueeze. If a file's size is greater than the threshold, it is copied directly
     * to the replica location without datasqueeze. If this property is not set in the copier-options, DataSqueeze
     * will use its default value.
     */
    private Long threshold = -1L;

    /**
     * Create a new DataSqueezeOptions instance from a map of options.
     *
     * @param copierOptions CopierOptions
     * @return New DataSqueezeOptions instance
     */
    public static DataSqueezeOptions parse(final Map<String, Object> copierOptions) {
        final DataSqueezeOptions compactionOptions = new DataSqueezeOptions();

        final Long threshold = MapUtils.getLong(copierOptions, THRESHOLD, compactionOptions.getThreshold());
        if (threshold != null) {
            compactionOptions.setThreshold(threshold);
        }

        return compactionOptions;
    }

    /**
     * Get DataSqueeze threshold setting.
     * @return Threshold
     */
    public Long getThreshold() {
        return this.threshold;
    }

    private void setThreshold(final Long threshold) {
        this.threshold = threshold;
    }
}
