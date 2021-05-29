package com.huc.batch.listener;


import com.huc.batch.pojo.BlogInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;

public class MyReadListener implements ItemReadListener<BlogInfo> {

    private Logger logger = LoggerFactory .getLogger(MyReadListener.class);

    @Override
    public void beforeRead() {

    }

    @Override
    public void afterRead(BlogInfo blogInfo) {

    }

    @Override
    public void onReadError(Exception e) {

    }
}
