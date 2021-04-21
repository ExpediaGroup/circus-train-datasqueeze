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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.dsp.data.squeeze.CompactionManager;
import com.expedia.dsp.data.squeeze.CompactionManagerFactory;
import com.expedia.dsp.data.squeeze.models.CompactionResponse;
import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

/**
 * DataSqueeze Copier
 *
 * Runs DataSqueeze to compact data from the source to replica location
 */
public class DataSqueezeCopier implements Copier {

    private static final Logger LOG = LoggerFactory.getLogger(DataSqueezeCopier.class);

    private final Configuration replicaConf;
    private final Path sourceDataLocation;
    private final Path replicaDataLocation;
    private final DataSqueezeOptions compactionOptions;

    private final DataSqueezeMetrics metrics = new DataSqueezeMetrics();

    public DataSqueezeCopier(final Configuration replicaConf,
                             final Path sourceDataLocation,
                             final Path replicaDataLocation,
                             final DataSqueezeOptions compactionOptions) {
        this.replicaConf = replicaConf;
        this.sourceDataLocation = sourceDataLocation;
        this.replicaDataLocation = replicaDataLocation;
        this.compactionOptions = compactionOptions;
    }

    public Metrics copy() {
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("sourcePath", this.sourceDataLocation.toString());
        optionsMap.put("targetPath", this.replicaDataLocation.toString());

        if (this.compactionOptions.getThreshold() > -1L) {
            optionsMap.put("thresholdInBytes", this.compactionOptions.getThreshold().toString());
        }

        boolean isSuccessful;
        try {
            final CompactionManager compactionManager = CompactionManagerFactory.create(optionsMap);
            final CompactionResponse compactionResponse = compactionManager.compact();

            isSuccessful = compactionResponse.isSuccessful();
        } catch (final Exception e) {
            throw new CircusTrainException("Unable to compact file(s)", e);
        } finally {
            this.cleanup();
        }

        this.metrics.setCompactionSuccess(isSuccessful);
        return this.metrics;
    }

    private void cleanup() {
        final URI sourceLocation = this.sourceDataLocation.toUri();
        LOG.info("Deleting temporary source location: {}", sourceLocation);

        try (final FileSystem fileSystem = FileSystem.get(sourceLocation, this.replicaConf)) {
            fileSystem.delete(this.sourceDataLocation, true);
        } catch (final IOException e) {
            throw new CircusTrainException("Unable to delete temporary file(s)", e);
        }

        LOG.info("Successfully deleted temporary source location: {}", sourceLocation);
    }
}
