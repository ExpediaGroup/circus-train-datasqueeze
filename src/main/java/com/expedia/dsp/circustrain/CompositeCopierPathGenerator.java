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
package com.expedia.dsp.circustrain;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.copier.CopierPathGenerator;
import com.hotels.bdp.circustrain.api.copier.CopierPathGeneratorParams;

/**
 * CopierPathGenerator that provides source and replica paths for each Copier
 * in a CompositeCopierFactory.
 *
 * It expects 2 Copiers, and uses a temp directory
 * configured in the copier-options.
 */
public class CompositeCopierPathGenerator implements CopierPathGenerator {

    static final String COMPOSITE_TMP_DIR = "composite-tmp-dir";

    private static final Logger LOG = LoggerFactory.getLogger(CompositeCopierPathGenerator.class);

    private Path tmpPath = null;

    @Override
    public Path generateSourceBaseLocation(final CopierPathGeneratorParams copierPathGeneratorParams) {
        final Path sourcePath;

        if (copierPathGeneratorParams.getCopierIndex() == 0) {
            // S3DistCpCopierFactory gets the initial source location
            sourcePath = copierPathGeneratorParams.getOriginalSourceBaseLocation();
        } else {
            // DataSqueezeCopier gets the previously-used temporary location
            sourcePath = this.getTmpPath(copierPathGeneratorParams.getOverridingCopierOptions());
        }

        LOG.info("Copier {} source directory is {}", copierPathGeneratorParams.getCopierIndex(), sourcePath);
        return sourcePath;
    }

    @Override
    public Path generateReplicaLocation(final CopierPathGeneratorParams copierPathGeneratorParams) {
        final Path replicaPath;

        if (copierPathGeneratorParams.getCopierIndex() < 1) {
            // S3DistCpCopierFactory gets a temporary location for staging the data before DataSqueeze
            replicaPath = this.getTmpPath(copierPathGeneratorParams.getOverridingCopierOptions());
        } else {
            // DataSqueezeCopier gets the final output location
            replicaPath = copierPathGeneratorParams.getOriginalReplicaLocation();
        }

        LOG.info("Copier {} replica directory is {}", copierPathGeneratorParams.getCopierIndex(), replicaPath);
        return replicaPath;
    }

    private Path getTmpPath(final Map<String, Object> overridingCopierOptions) {
        if (this.tmpPath == null) {
            final String tmp = MapUtils.getString(overridingCopierOptions, COMPOSITE_TMP_DIR, "/tmp");
            this.tmpPath = new Path(tmp, UUID.randomUUID().toString());
        }

        return this.tmpPath;
    }
}
