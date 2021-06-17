package org.embulk.decoder;

import org.embulk.spi.Exec;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.FileInput;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.file.FileInputInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonsCompressDecoderPlugin
        implements DecoderPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface PluginTask
            extends Task
    {
        @Config("format")
        @ConfigDefault("\"\"")
        public String getFormat();

        @Config("decompress_concatenated")
        @ConfigDefault("true")
        public boolean getDecompressConcatenated();

        @Config("match_name")
        @ConfigDefault("\"\"")
        public String getMatchName();

    }

    @Override
    public void transaction(ConfigSource config, DecoderPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        control.run(task.dump());
    }

    @Override
    public FileInput open(TaskSource taskSource, FileInput input)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        return new CommonsCompressFileInput(
                Exec.getBufferAllocator(),
                new CommonsCompressProvider(task, new FileInputInputStream(input) {
                    // NOTE: This is workaround code to avoid hanging issue.
                    // This issue will be fixed after merging #112.
                    // https://github.com/embulk/embulk/pull/112
                    @Override
                    public long skip(long len) {
                        long skipped = super.skip(len);
                        return skipped > 0 ? skipped : 0;
                    }
                }));
    }
}
