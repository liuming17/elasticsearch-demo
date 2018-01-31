package com.elasticsearch.demo.unit02;

import com.elasticsearch.demo.ElasticsearchApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by liuming on 2017/12/6.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes= ElasticsearchApplication.class)
public class ElasticsearchScanImportServiceTest {

    @Autowired
    private ElasticsearchScanImportService elasticsearchScanImportService ;

    @Test
    public void importAll() throws Exception {
        elasticsearchScanImportService.importAll("trade" , "trade3" , 1000);
    }

}