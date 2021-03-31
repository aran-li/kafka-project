package com.test.structured_streaming.pojo;

import lombok.Data;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/3/30 17:13
 */
@Data
public class FBean {
    public Integer partition;
    public Long  offset;
}
