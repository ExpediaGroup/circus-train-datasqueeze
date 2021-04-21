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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.hotels.bdp.circustrain.api.copier.CopierPathGeneratorParams;

/**
 * Tests for {@link CompositeCopierPathGeneratorTest}
 */
public final class CompositeCopierPathGeneratorTest {

    @Test
    public void testGenerateSourceBaseLocationCopier0() {
        final CompositeCopierPathGenerator generator = new CompositeCopierPathGenerator();

        final Map<String, Object> configOptions = new HashMap<>();
        configOptions.put(CompositeCopierPathGenerator.COMPOSITE_TMP_DIR, "/tmp");
        final CopierPathGeneratorParams params = CopierPathGeneratorParams.newParams(0, "id",
                new Path("/source"),
                new ArrayList<Path>(),
                new Path("/dest"),
                configOptions);
        final Path replicaLocation = generator.generateSourceBaseLocation(params);
        assertThat(replicaLocation.toString(), is("/source"));
    }

    @Test
    public void testGenerateSourceBaseLocationCopier1() {
        final CompositeCopierPathGenerator generator = new CompositeCopierPathGenerator();

        final Map<String, Object> configOptions = new HashMap<>();
        configOptions.put(CompositeCopierPathGenerator.COMPOSITE_TMP_DIR, "/tmp");
        final CopierPathGeneratorParams params = CopierPathGeneratorParams.newParams(1, "id",
                new Path("/source"),
                new ArrayList<Path>(),
                new Path("/dest"),
                configOptions);
        final Path replicaLocation = generator.generateSourceBaseLocation(params);
        assertThat(replicaLocation.toString(), startsWith("/tmp/"));
    }

    @Test
    public void testGenerateReplicaLocationCopier0() {
        final CompositeCopierPathGenerator generator = new CompositeCopierPathGenerator();

        final Map<String, Object> configOptions = new HashMap<>();
        configOptions.put(CompositeCopierPathGenerator.COMPOSITE_TMP_DIR, "/tmp");
        final CopierPathGeneratorParams params = CopierPathGeneratorParams.newParams(0, "id",
                new Path("/source"),
                new ArrayList<Path>(),
                new Path("/dest"),
                configOptions);
        final Path replicaLocation = generator.generateReplicaLocation(params);
        assertThat(replicaLocation.toString(), startsWith("/tmp"));
    }

    @Test
    public void testGenerateReplicaLocationCopier1() {
        final CompositeCopierPathGenerator generator = new CompositeCopierPathGenerator();

        final Map<String, Object> configOptions = new HashMap<>();
        configOptions.put(CompositeCopierPathGenerator.COMPOSITE_TMP_DIR, "/tmp");
        final CopierPathGeneratorParams params = CopierPathGeneratorParams.newParams(1, "id",
                new Path("/source"),
                new ArrayList<Path>(),
                new Path("/dest"),
                configOptions);
        final Path replicaLocation = generator.generateReplicaLocation(params);
        assertThat(replicaLocation.toString(), is("/dest"));
    }

    @Test
    public void testReuseTmpPath() {
        final CompositeCopierPathGenerator generator = new CompositeCopierPathGenerator();

        final Map<String, Object> configOptions = new HashMap<>();
        configOptions.put(CompositeCopierPathGenerator.COMPOSITE_TMP_DIR, "/tmp");

        final CopierPathGeneratorParams params0 = CopierPathGeneratorParams.newParams(0, "id",
                new Path("/source"),
                new ArrayList<Path>(),
                new Path("/dest"),
                configOptions);
        final Path replicaLocation = generator.generateReplicaLocation(params0);

        final CopierPathGeneratorParams params1 = CopierPathGeneratorParams.newParams(1, "id",
                new Path("/source"),
                new ArrayList<Path>(),
                new Path("/dest"),
                configOptions);
        final Path sourceBaseLocation = generator.generateSourceBaseLocation(params1);


        assertThat(replicaLocation.toString(), is(sourceBaseLocation.toString()));
    }

}
