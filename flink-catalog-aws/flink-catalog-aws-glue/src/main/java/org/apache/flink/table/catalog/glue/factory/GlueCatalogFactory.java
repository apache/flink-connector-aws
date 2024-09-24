/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.factory;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.glue.GlueCatalog;
import org.apache.flink.table.catalog.glue.GlueCatalogOptions;
import org.apache.flink.table.catalog.glue.util.GlueCatalogOptionsUtils;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** Catalog factory for {@link GlueCatalog}. */
@PublicEvolving
public class GlueCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GlueCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return GlueCatalogOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> allConfigs = GlueCatalogOptions.getAllConfigOptions();
        allConfigs.removeAll(GlueCatalogOptions.getRequiredConfigOptions());
        return allConfigs;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return GlueCatalogOptions.getRequiredConfigOptions();
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        GlueCatalogOptionsUtils optionsUtils =
                new GlueCatalogOptionsUtils(context.getOptions(), context.getConfiguration());
        helper.validateExcept(optionsUtils.getNonValidatedPrefixes().toArray(new String[0]));
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    context.getOptions().entrySet().stream()
                            .map(entry -> entry.getKey() + "-> " + entry.getValue())
                            .collect(Collectors.joining("\n")));
        }
        Properties glueCatalogValidatedProperties = optionsUtils.getValidatedConfigurations();
        return new GlueCatalog(
                context.getName(),
                helper.getOptions().get(GlueCatalogOptions.DEFAULT_DATABASE),
                context.getConfiguration(),
                glueCatalogValidatedProperties);
    }
}
