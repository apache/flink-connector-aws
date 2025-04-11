package org.apache.apache.flink.table.catalog.glue;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GlueCatalogFactory implements CatalogFactory {

    // Define configuration options that users must provide
    public static final ConfigOption<String> REGION =
            ConfigOptions.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region for the Glue catalog");

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Default database to use in Glue catalog");

    @Override
    public String factoryIdentifier() {
        return "glue"; // This allows users to use `type = 'glue'` in SQL
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REGION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        // Read the configuration options
        Map<String,String> config = context.getOptions();
        String name = context.getName();
        String region = config.get("region");
        String defaultDatabase = config.get("default-database");

        // Ensure required properties are present
        if (region == null || region.isEmpty()) {
            throw new CatalogException("The 'region' property must be specified for the Glue catalog.");
        }

        // Create and return the GlueCatalog instance
        return new GlueCatalog(name, defaultDatabase, region);
    }
}