package dev.darrencodes.parquetdemo;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Simple POJO to save and read from the Parquet file.
 *
 * @author darren
 */
@Getter
@Setter
@Accessors(chain = true)
public class Message {

    private long id;
    private String userId;

    @Override
    public String toString() {
        return this.userId;
    }
}
