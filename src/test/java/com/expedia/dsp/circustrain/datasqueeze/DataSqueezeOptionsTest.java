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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Tests for {@link DataSqueezeOptions}
 */
public final class DataSqueezeOptionsTest {

    @Test
    public void testThresholdValid() {
        final Map<String, Object> copierOptions = new HashMap<>();
        copierOptions.put(DataSqueezeOptions.THRESHOLD, "1222444");
        final DataSqueezeOptions options = DataSqueezeOptions.parse(copierOptions);
        assertNotNull(options);
        assertThat(options.getThreshold(), is(1222444L));
    }

    @Test
    public void testThresholdInvalid() {
        final Map<String, Object> copierOptions = new HashMap<>();
        copierOptions.put(DataSqueezeOptions.THRESHOLD, "default");
        final DataSqueezeOptions options = DataSqueezeOptions.parse(copierOptions);
        assertNotNull(options);
        assertThat(options.getThreshold(), is(-1L));
    }

}
