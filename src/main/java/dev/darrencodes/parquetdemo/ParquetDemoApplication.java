package dev.darrencodes.parquetdemo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Driver application.
 *
 * @author darren
 */
@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class ParquetDemoApplication implements CommandLineRunner {
    private final ParquetConfiguration parquetConfiguration;

    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(ParquetDemoApplication.class, args);
    }

    /**
     * Called by Spring to do the actual processing of the application.
     *
     * @param args Command line arguments.
     * @throws IOException Any error will be propagated.
     */
    @Override
    public void run(String... args) throws IOException {

        // Remove the output from previous runs.
        File f = new File(this.parquetConfiguration.getFileName());
        if (f.exists() && !f.delete()) {
            log.error("Unable to delete file");
            throw new IOException(String.format("Unable to delete source data file at %s.", this.parquetConfiguration.getFileName()));
        }

        Path filePath = new Path(this.parquetConfiguration.getFileName());
        Configuration configuration = new Configuration();

        writeData(filePath, configuration);
        readData(filePath, configuration);
    }

    // Writes sample data to the Parquet file.
    private void writeData(Path filePath, Configuration configuration) throws IOException {

        // Use ReflectData.AllowNull if you want nullable fields written to the file. This will throw an error if any fields are null.
        ReflectData reflectData = ReflectData.get();

        AvroParquetWriter.Builder<Message> writerBuilder = AvroParquetWriter.<Message>builder(HadoopOutputFile.fromPath(filePath, configuration))
                .withSchema(reflectData.getSchema(Message.class))
                .withDataModel(reflectData)
                .withPageSize(this.parquetConfiguration.getPageSize())
                .withCompressionCodec(this.parquetConfiguration.getCompressionCodecName());

        try (ParquetWriter<Message> parquetWriter = writerBuilder.build()) {
            for (Message message : getSampleData(500)) {
                parquetWriter.write(message);
            }
        }
    }

    // Reads the sample data from the Parquet file.
    private void readData(Path filePath, Configuration configuration) throws IOException {

        ParquetReader.Builder<Message> readerBuilder = AvroParquetReader.<Message>builder(HadoopInputFile.fromPath(filePath, configuration))
                // Other examples passed Message.class.getClassLoader() to the ReflectData constructor.
                .withDataModel(ReflectData.get())
                .disableCompatibility();

        try (ParquetReader<Message> recordParquetReader = readerBuilder.build()) {

            Message messageRecord = recordParquetReader.read();
            while (messageRecord != null) {
                log.info(messageRecord.toString());
                messageRecord = recordParquetReader.read();
            }
        }
    }

    // Generates sample data.
    private static List<Message> getSampleData(int count) {

        List<Message> messages = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            messages.add(new Message().setId(i).setUserId(String.format("user-%d", i)));
        }

        return messages;
    }
}
