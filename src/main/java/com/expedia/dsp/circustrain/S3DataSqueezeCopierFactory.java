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
package com.expedia.dsp.circustrain;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.expedia.dsp.circustrain.datasqueeze.DataSqueezeCopierFactory;
import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.copier.CompositeCopierFactory;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.MetricsMerger;
import com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpCopierFactory;

/**
 * S3DataSqueezeCopierFactory -- this factory creates copiers for Circus Train.
 *
 * Each copier instance is a CompositeCopier composed of S3MapReduceCpCopier and DataSqueezeCopier.
 */
@Profile(Modules.REPLICATION)
@Component
public class S3DataSqueezeCopierFactory implements CopierFactory {

    private final S3MapReduceCpCopierFactory s3MapReduceCpCopierFactory;
    private final Configuration replicaHiveConf;

    @Autowired
    public S3DataSqueezeCopierFactory(final S3MapReduceCpCopierFactory s3MapReduceCpCopierFactory,
                                      @Value("#{replicaHiveConf}") final Configuration replicaHiveConf) {
        this.s3MapReduceCpCopierFactory = s3MapReduceCpCopierFactory;
        this.replicaHiveConf = replicaHiveConf;
    }

    @Override
    public boolean supportsSchemes(final String sourceScheme, final String replicaScheme) {
        return this.s3MapReduceCpCopierFactory.supportsSchemes(sourceScheme, replicaScheme);
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

        final List<CopierFactory> delegates = Arrays.asList(
                this.s3MapReduceCpCopierFactory,
                new DataSqueezeCopierFactory(this.replicaHiveConf));

        final CompositeCopierFactory factory = new CompositeCopierFactory(delegates,
                new CompositeCopierPathGenerator(), MetricsMerger.DEFAULT);

        return factory.newInstance(eventId, sourceBaseLocation, replicaLocation, overridingCopierOptions);
    }

}
