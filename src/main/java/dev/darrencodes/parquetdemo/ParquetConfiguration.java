package dev.darrencodes.parquetdemo;

import lombok.Getter;
import lombok.Setter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Holds properties for the Parquet reader and writer.
 *
 * @author darren
 */
@Configuration
@ConfigurationProperties(prefix = "dev.darrencodes.parquet-demo")
@Getter
@Setter
public class ParquetConfiguration {

    private String fileName;
    private int pageSize;
    private CompressionCodecName compressionCodecName;
}
