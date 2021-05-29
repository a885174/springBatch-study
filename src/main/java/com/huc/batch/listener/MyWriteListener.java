package com.huc.batch.listener;

import com.huc.batch.pojo.BlogInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.batch.api.chunk.listener.ItemWriteListener;
import java.util.List;

import static java.lang.String.format;

public class MyWriteListener implements ItemWriteListener {

    private Logger logger= LoggerFactory.getLogger(ItemWriteListener.class);

    @Override
    public void beforeWrite(List<Object> list) throws Exception {

    }

    @Override
    public void afterWrite(List<Object> list) throws Exception {

    }

    @Override
    public void onWriteError(List<Object> list, Exception e) throws Exception {
        logger.info(format("%s%n", e.getMessage()));
        for (Object ob  : list) {
            BlogInfo message= (BlogInfo) ob;
            logger.info(format("Failed writing BlogInfo : %s", message.toString()));
        }

    }


}
