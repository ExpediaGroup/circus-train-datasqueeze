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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;

/**
 * Factory to initialize the DataSqueezeCopier.
 */
public class DataSqueezeCopierFactory implements CopierFactory {

    private final Configuration conf;

    public DataSqueezeCopierFactory(final Configuration conf) {
        this.conf = conf;
    }

    @Override
    public boolean supportsSchemes(final String sourceScheme, final String replicaScheme) {
        return true;
    }

    @Override
    public Copier newInstance(
            final String eventId,
            final Path sourceBaseLocation,
            final List<Path> sourceSubLocations,
            final Path replicaLocation,
            final Map<String, Object> overridingCopierOptions) {
        return this.newInstance(eventId, sourceBaseLocation, replicaLocation, overridingCopierOptions);
    }

    @Override
    public Copier newInstance(
            final String eventId,
            final Path sourceBaseLocation,
            final Path replicaLocation,
            final Map<String, Object> overridingCopierOptions) {
        final DataSqueezeOptions compactionOptions = DataSqueezeOptions.parse(overridingCopierOptions);
        return new DataSqueezeCopier(this.conf, sourceBaseLocation, replicaLocation, compactionOptions);
    }

}
