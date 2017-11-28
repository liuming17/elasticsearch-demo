package com.elasticsearch.demo.unit01;

import com.elasticsearch.demo.ElasticsearchApplication;
import com.elasticsearch.demo.vo.Employee;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuming on 2017/11/12.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes= ElasticsearchApplication.class)
public class ElasticsearchCurdServiceTest {


    private final Logger logger = LoggerFactory.getLogger(getClass()) ;

    final String index = "megacorp" ;
    final String type = "employee" ;

    @Autowired
    private ElasticsearchCurdService elasticsearchCurdService ;

    @Test
    public void addDemo() throws Exception {
        Employee employee = new Employee() ;
        /*employee.setId(1L);
        employee.setFirst_ame("John");
        employee.setLast_name("Smith");
        employee.setAge(25);
        employee.setAbout("I love to go rock climbing");
        List<String> interests = new ArrayList<>() ;
        interests.add("sports") ;
        interests.add("music") ;
        employee.setInterests(interests);*/

        /*employee.setId(2L);
        employee.setFirst_ame("Jane");
        employee.setLast_name("Smith");
        employee.setAge(32);
        employee.setAbout("I like to collect rock albums");
        List<String> interests = new ArrayList<>() ;
        interests.add("music") ;
        employee.setInterests(interests);*/

        employee.setId(3L);
        employee.setFirst_ame("Douglas");
        employee.setLast_name("Fir");
        employee.setAge(35);
        employee.setAbout("I like to build cabinets");
        List<String> interests = new ArrayList<>() ;
        interests.add("forestry") ;
        employee.setInterests(interests);

        elasticsearchCurdService.add(index, type, employee);
    }

    @Test
    public void getById(){
        Long id = 1L ;
        elasticsearchCurdService.getById(index, type, id.toString()) ;
    }

    @Test
    public void getAll(){
        elasticsearchCurdService.getAll(index, type) ;
    }

    @Test
    public void getByQueryString()throws Exception{
        elasticsearchCurdService.getByQueryString(index, type, "last_name:Smith") ;
    }

    @Test
    public void getByQueryMatchDsl()throws Exception{
        elasticsearchCurdService.getByQueryMatchDsl(index, type, "last_name", "Smith") ;

        elasticsearchCurdService.getByQueryMatchDsl(index, type, "about", "rock climbing") ;
    }

    @Test
    public void getByMatchAndFilter(){
        elasticsearchCurdService.getByMatchAndFilter(index, type, "last_name", "Smith", "age" , 30) ;
    }

    @Test
    public void getByQueryMatchPhraseDsl(){
        elasticsearchCurdService.getByQueryMatchPhraseDsl(index, type, "about", "rock climbing") ;
    }

    @Test
    public void getByQueryMatchPhraseAndHighlightDsl() throws Exception {
        elasticsearchCurdService.getByQueryMatchPhraseAndHighlightDsl(index, type, "about", "rock climbing") ;
    }

    @Test
    public void group(){
        elasticsearchCurdService.group(index, type, "interests") ;
    }

    @Test
    public void groupAndQuery(){
        elasticsearchCurdService.groupAndQuery(index, type, "interests", "last_name", "smith") ;
    }

    @Test
    public void groupAndAvg(){
        elasticsearchCurdService.groupAndAvg(index, type, "interests", "age") ;
    }

}